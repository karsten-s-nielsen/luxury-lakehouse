# D53 + SEC3: Wheel Version Management & SHA-256 Pinning — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a single source of truth for wheel version + SHA-256 hash, fix 16 stale 0.1.0 references to 0.3.0, and automate future version bumps with supply-chain hash pinning.

**Architecture:** `src/shared/wheel.py` holds the canonical `WHEEL_VERSION` constant and pure rewrite functions (stdlib only, safe for all packages per import-linter). `scripts/bump_wheel.py` reads the version from `pyproject.toml` and propagates it to all static consumers (14 PEP 723 scripts, deploy.sh, Terraform). Dynamic consumers (`evolve/backends/hf_jobs.py`, `benchmark_hf_jobs.py`, `manage_space.py`, `deploy_wheel.py`) import from `shared.wheel` directly — they never go stale. SHA-256 hashes are computed from the HF Hub-uploaded wheel and pinned into PEP 723 URL fragments (`#sha256=HASH`) via `bump_wheel.py --pin-hash`.

**Tech Stack:** Python stdlib (`re`, `pathlib`, `hashlib`, `importlib.metadata`), hatchling build, HF Hub API, GitHub Actions CI

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/shared/wheel.py` | Create | Version constant, URL construction, rewrite functions |
| `scripts/bump_wheel.py` | Create | CLI: propagate version from pyproject.toml to all static consumers |
| `src/tests/test_wheel_constants.py` | Create | Unit tests for wheel.py constants + rewrite functions |
| `src/tests/test_wheel_conformance.py` | Create | Integration tests: all consumers reference correct version |
| `src/evolve/backends/hf_jobs.py` | Modify :38-43 | Import WHEEL_BASE_URL, f-string worker script |
| `scripts/benchmark_hf_jobs.py` | Modify :70-76 | Import WHEEL_BASE_URL, f-string worker script |
| `scripts/manage_space.py` | Modify :169-181 | Import WHEEL_FILENAME, exact-match instead of glob |
| `scripts/deploy_wheel.py` | Modify :58-69 | Import WHEEL_FILENAME, remove `_find_wheel_filename()` |
| 14× `scripts/*_hf.py` | Modify :4 | `0.1.0` → `0.3.0` (via bump_wheel.py) |
| `scripts/deploy.sh` | Modify :25 | `0.1.0` → `0.3.0` (via bump_wheel.py) |
| `terraform/environments/dev/main.tf` | No change | Already at `0.3.0` |
| `terraform/modules/workflows/variables.tf` | Modify :17 | Description example `0.1.0` → `0.3.0` (via bump_wheel.py) |
| `.github/workflows/python-ci.yml` | Modify :60-73 | Add hash sidecar, stale cleanup, version check |

---

### Task 1: Create `src/shared/wheel.py` — constants and rewrite functions

**Files:**
- Create: `src/shared/wheel.py`
- Create: `src/tests/test_wheel_constants.py`

- [ ] **Step 1: Write failing unit tests**

```python
# src/tests/test_wheel_constants.py
"""Tests for wheel version constants and rewrite functions."""

from __future__ import annotations

import importlib.metadata
import re
from pathlib import Path

import pytest


class TestWheelConstants:
    """WHEEL_VERSION must stay in sync with pyproject.toml."""

    def test_version_matches_pyproject(self) -> None:
        from shared.wheel import WHEEL_VERSION

        expected = importlib.metadata.version("luxury-lakehouse")
        assert WHEEL_VERSION == expected, (
            f"WHEEL_VERSION={WHEEL_VERSION!r} != pyproject.toml version={expected!r}. "
            "Run: uv run python scripts/bump_wheel.py"
        )

    def test_filename_format(self) -> None:
        from shared.wheel import WHEEL_FILENAME, WHEEL_VERSION

        assert WHEEL_FILENAME == f"luxury_lakehouse-{WHEEL_VERSION}-py3-none-any.whl"

    def test_base_url_format(self) -> None:
        from shared.wheel import WHEEL_BASE_URL, WHEEL_FILENAME

        assert WHEEL_BASE_URL.startswith("https://huggingface.co/")
        assert WHEEL_BASE_URL.endswith(WHEEL_FILENAME)
        assert "/resolve/main/" in WHEEL_BASE_URL

    def test_repo_constant(self) -> None:
        from shared.wheel import WHEEL_REPO

        assert WHEEL_REPO == "luxury-lakehouse/build-artifacts"


class TestRewriteWheelUrl:
    """rewrite_wheel_url replaces wheel filename references."""

    def test_basic_replacement(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = '#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",'
        result = rewrite_wheel_url(text, "0.3.0")
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl" in result
        assert "luxury_lakehouse-0.1.0-py3-none-any.whl" not in result

    def test_preserves_surrounding_text(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = '#     "luxury-lakehouse[analytics,training] @ https://example.com/luxury_lakehouse-0.1.0-py3-none-any.whl",'
        result = rewrite_wheel_url(text, "0.3.0")
        assert "[analytics,training]" in result
        assert result.startswith('#     "luxury-lakehouse[analytics,training]')
        assert result.endswith('.whl",')

    def test_adds_sha256_fragment(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "luxury_lakehouse-0.1.0-py3-none-any.whl"
        result = rewrite_wheel_url(text, "0.3.0", sha256="abcdef1234567890" * 4)
        assert f"luxury_lakehouse-0.3.0-py3-none-any.whl#sha256={'abcdef1234567890' * 4}" in result  # pragma: allowlist secret

    def test_replaces_existing_hash(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "luxury_lakehouse-0.2.0-py3-none-any.whl#sha256=oldhashvalue1234567890abcdef1234567890abcdef1234567890abcdef12345678"
        result = rewrite_wheel_url(text, "0.3.0", sha256="newhash" + "0" * 57)
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl#sha256=newhash" in result
        assert "oldhash" not in result

    def test_terraform_path(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = 'wheel_path = "${module.catalog.libs_volume_path}/luxury_lakehouse-0.3.0-py3-none-any.whl"'
        result = rewrite_wheel_url(text, "0.4.0")
        assert "luxury_lakehouse-0.4.0-py3-none-any.whl" in result

    def test_deploy_sh(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = 'WHEEL_NAME="luxury_lakehouse-0.1.0-py3-none-any.whl"'
        result = rewrite_wheel_url(text, "0.3.0")
        assert 'luxury_lakehouse-0.3.0-py3-none-any.whl' in result

    def test_no_match_returns_unchanged(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "no wheel reference here"
        assert rewrite_wheel_url(text, "0.3.0") == text

    def test_multiple_occurrences(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "luxury_lakehouse-0.1.0-py3-none-any.whl\nluxury_lakehouse-0.2.0-py3-none-any.whl"
        result = rewrite_wheel_url(text, "0.3.0")
        assert result.count("luxury_lakehouse-0.3.0-py3-none-any.whl") == 2


class TestRewriteWheelVersionConstant:
    """rewrite_wheel_version_constant updates the WHEEL_VERSION line."""

    def test_basic(self) -> None:
        from shared.wheel import rewrite_wheel_version_constant

        text = 'WHEEL_VERSION = "0.2.0"\n'
        result = rewrite_wheel_version_constant(text, "0.3.0")
        assert 'WHEEL_VERSION = "0.3.0"' in result
        assert "0.2.0" not in result

    def test_preserves_surrounding(self) -> None:
        from shared.wheel import rewrite_wheel_version_constant

        text = '"""Docstring."""\n\nWHEEL_VERSION = "0.1.0"\n\nWHEEL_REPO = "foo"\n'
        result = rewrite_wheel_version_constant(text, "0.3.0")
        assert '"""Docstring."""' in result
        assert 'WHEEL_REPO = "foo"' in result
        assert 'WHEEL_VERSION = "0.3.0"' in result


class TestReadPyprojectVersion:
    """read_pyproject_version extracts version from pyproject.toml."""

    def test_reads_version(self, tmp_path: Path) -> None:
        from shared.wheel import read_pyproject_version

        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "luxury-lakehouse"\nversion = "1.2.3"\n'
        )
        assert read_pyproject_version(tmp_path) == "1.2.3"

    def test_raises_on_missing(self, tmp_path: Path) -> None:
        from shared.wheel import read_pyproject_version

        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        with pytest.raises(ValueError, match="Could not find version"):
            read_pyproject_version(tmp_path)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_wheel_constants.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'shared.wheel'`

- [ ] **Step 3: Write implementation**

```python
# src/shared/wheel.py
"""Wheel version constants and rewrite utilities.

Single source of truth for the wheel version and HF Hub URL.
This module has zero external dependencies — stdlib only.

To bump the version, run: uv run python scripts/bump_wheel.py
"""

from __future__ import annotations

import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WHEEL_VERSION = "0.3.0"
"""Must match the version in pyproject.toml. Enforced by test_wheel_constants.py."""

WHEEL_REPO = "luxury-lakehouse/build-artifacts"
"""HF Hub repository hosting the pre-built wheel."""

WHEEL_FILENAME = f"luxury_lakehouse-{WHEEL_VERSION}-py3-none-any.whl"
"""Wheel filename on HF Hub (PEP 427 format)."""

WHEEL_BASE_URL = (
    f"https://huggingface.co/{WHEEL_REPO}/resolve/main/{WHEEL_FILENAME}"
)
"""Direct download URL for the wheel on HF Hub (no hash pinning)."""

# ---------------------------------------------------------------------------
# Compiled patterns (module-level per CLAUDE.md)
# ---------------------------------------------------------------------------

_WHEEL_URL_RE: re.Pattern[str] = re.compile(
    r"luxury_lakehouse-\d+\.\d+\.\d+-py3-none-any\.whl"
    r"(#sha256=[a-f0-9]+)?"
)
"""Matches any versioned wheel filename, with optional SHA-256 fragment."""

_WHEEL_VERSION_RE: re.Pattern[str] = re.compile(
    r'WHEEL_VERSION\s*=\s*"\d+\.\d+\.\d+"'
)
"""Matches the WHEEL_VERSION constant assignment in this file."""

_PYPROJECT_VERSION_RE: re.Pattern[str] = re.compile(
    r'^version\s*=\s*"([^"]+)"', re.MULTILINE
)
"""Matches the version field in pyproject.toml [project] section."""

# ---------------------------------------------------------------------------
# Pure rewrite functions
# ---------------------------------------------------------------------------


def rewrite_wheel_url(text: str, version: str, sha256: str | None = None) -> str:
    """Replace all wheel filename references with the given version and optional hash.

    Handles PEP 723 headers, Terraform paths, shell scripts, and any other
    context containing ``luxury_lakehouse-X.Y.Z-py3-none-any.whl``.
    """
    replacement = f"luxury_lakehouse-{version}-py3-none-any.whl"
    if sha256:
        replacement += f"#sha256={sha256}"
    return _WHEEL_URL_RE.sub(replacement, text)


def rewrite_wheel_version_constant(text: str, version: str) -> str:
    """Replace ``WHEEL_VERSION = "X.Y.Z"`` with the new version."""
    return _WHEEL_VERSION_RE.sub(f'WHEEL_VERSION = "{version}"', text)


def read_pyproject_version(project_root: Path) -> str:
    """Read the ``version`` field from ``pyproject.toml``."""
    pyproject = project_root / "pyproject.toml"
    match = _PYPROJECT_VERSION_RE.search(pyproject.read_text(encoding="utf-8"))
    if not match:
        msg = f"Could not find version in {pyproject}"
        raise ValueError(msg)
    return match.group(1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_wheel_constants.py -v`
Expected: All 14 tests PASS

- [ ] **Step 5: Run lint + type check**

Run: `uv run ruff check src/shared/wheel.py src/tests/test_wheel_constants.py && uv run pyright src/shared/wheel.py`
Expected: PASS

- [ ] **Step 6: Verify import boundary compliance**

Run: `uv run lint-imports`
Expected: PASS — `shared.wheel` uses stdlib only

---

### Task 2: Create `scripts/bump_wheel.py` — CLI version propagation

**Files:**
- Create: `scripts/bump_wheel.py`

This is a developer/CI tool, not a wheel-shipped module. The pure functions it calls (rewrite logic) are tested in Task 1. This task tests the CLI integration by running it in `--check` and `--dry-run` modes.

- [ ] **Step 1: Write the CLI script**

```python
#!/usr/bin/env python3
"""Propagate wheel version from pyproject.toml to all consumers.

Usage:
    uv run python scripts/bump_wheel.py              # Sync all files
    uv run python scripts/bump_wheel.py --check      # Check consistency (CI)
    uv run python scripts/bump_wheel.py --dry-run    # Preview changes
    uv run python scripts/bump_wheel.py --pin-hash SHA256  # Add hash to PEP 723 URLs
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from shared.wheel import (
    WHEEL_VERSION,
    _WHEEL_URL_RE,
    read_pyproject_version,
    rewrite_wheel_url,
    rewrite_wheel_version_constant,
)

logger = logging.getLogger(__name__)

# Glob patterns for discovering static consumers (files with hardcoded wheel URLs).
# Dynamic consumers (evolve, benchmark, manage_space, deploy_wheel) use imports
# and are NOT in this list — they update automatically.
_CONSUMER_GLOBS = [
    "scripts/*_hf.py",
    "scripts/deploy.sh",
    "terraform/**/*.tf",
]

# Files to skip during discovery.
_EXCLUDE_NAMES = frozenset({"bump_wheel.py"})


def _discover_consumers(project_root: Path) -> list[Path]:
    """Find all files containing hardcoded wheel filename references."""
    consumers: list[Path] = []
    for pattern in _CONSUMER_GLOBS:
        for path in sorted(project_root.glob(pattern)):
            if path.name in _EXCLUDE_NAMES:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, PermissionError):
                continue
            if _WHEEL_URL_RE.search(text):
                consumers.append(path)
    return consumers


def _sync(
    project_root: Path,
    *,
    dry_run: bool = False,
    sha256: str | None = None,
) -> int:
    """Sync all consumer files to the pyproject.toml version.

    Returns count of changed files.
    """
    version = read_pyproject_version(project_root)
    changed = 0

    # 1. Update WHEEL_VERSION constant in wheel.py
    wheel_py = project_root / "src" / "shared" / "wheel.py"
    old_text = wheel_py.read_text(encoding="utf-8")
    new_text = rewrite_wheel_version_constant(old_text, version)
    if old_text != new_text:
        if not dry_run:
            wheel_py.write_text(new_text, encoding="utf-8")
        logger.info(
            "%s %s",
            "Would update" if dry_run else "Updated",
            wheel_py.relative_to(project_root),
        )
        changed += 1

    # 2. Update all static consumers
    for path in _discover_consumers(project_root):
        old_text = path.read_text(encoding="utf-8")
        new_text = rewrite_wheel_url(old_text, version, sha256=sha256)
        if old_text != new_text:
            if not dry_run:
                path.write_text(new_text, encoding="utf-8")
            logger.info(
                "%s %s",
                "Would update" if dry_run else "Updated",
                path.relative_to(project_root),
            )
            changed += 1

    return changed


def _check(project_root: Path) -> int:
    """Check version consistency. Returns 0 if consistent, 1 if stale."""
    version = read_pyproject_version(project_root)

    # Check wheel.py constant
    if WHEEL_VERSION != version:
        logger.error(
            "src/shared/wheel.py WHEEL_VERSION=%r != pyproject.toml version=%r",
            WHEEL_VERSION,
            version,
        )
        return 1

    # Check static consumers for stale references
    current_filename = f"luxury_lakehouse-{version}-py3-none-any.whl"
    stale: list[str] = []
    for path in _discover_consumers(project_root):
        text = path.read_text(encoding="utf-8")
        for match in _WHEEL_URL_RE.finditer(text):
            matched = match.group(0)
            # Strip optional hash fragment for version comparison
            filename_part = matched.split("#")[0]
            if filename_part != current_filename:
                stale.append(str(path.relative_to(project_root)))
                break

    if stale:
        logger.error(
            "Stale wheel references (expected %s):\n  %s",
            current_filename,
            "\n  ".join(stale),
        )
        return 1

    all_consumers = _discover_consumers(project_root)
    logger.info("All %d static consumers reference %s", len(all_consumers), current_filename)
    return 0


def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Propagate wheel version from pyproject.toml to all consumers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check consistency without modifying files (exit 1 if stale)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing files",
    )
    parser.add_argument(
        "--pin-hash",
        metavar="SHA256",
        help="Pin SHA-256 hash in PEP 723 URL fragments",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    project_root = Path(__file__).resolve().parent.parent

    if args.check:
        sys.exit(_check(project_root))

    changed = _sync(project_root, dry_run=args.dry_run, sha256=args.pin_hash)
    if changed == 0:
        logger.info("All files already up to date")
    else:
        action = "Would update" if args.dry_run else "Updated"
        logger.info("%s %d file(s)", action, changed)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it runs in `--check` mode (currently fails — stale files)**

Run: `uv run python scripts/bump_wheel.py --check`
Expected: Exit 1, listing stale files (all PEP 723 scripts still at 0.1.0)

- [ ] **Step 3: Verify `--dry-run` mode shows expected changes**

Run: `uv run python scripts/bump_wheel.py --dry-run`
Expected: Lists ~16 files that would be updated, no files actually changed

- [ ] **Step 4: Run lint**

Run: `uv run ruff check scripts/bump_wheel.py`
Expected: PASS

---

### Task 3: Conformance tests — version consistency across all consumers

**Files:**
- Create: `src/tests/test_wheel_conformance.py`

These tests catch version drift in CI. If someone bumps `pyproject.toml` but forgets `bump_wheel.py`, these fail.

- [ ] **Step 1: Write conformance tests (will be RED until Tasks 4-5 fix the files)**

```python
# src/tests/test_wheel_conformance.py
"""Conformance tests: all wheel consumers reference the correct version.

These tests enforce that every file referencing the luxury-lakehouse wheel
uses the version declared in src/shared/wheel.py. If a test fails, run:

    uv run python scripts/bump_wheel.py
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# Project root: src/tests/this_file.py → src/tests → src → project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class TestPep723ScriptsVersion:
    """All PEP 723 HF Jobs scripts must reference the current wheel version."""

    def test_pep723_scripts_use_current_version(self) -> None:
        from shared.wheel import WHEEL_FILENAME

        scripts_dir = _PROJECT_ROOT / "scripts"
        stale: list[str] = []
        for py_file in sorted(scripts_dir.glob("*_hf.py")):
            text = py_file.read_text(encoding="utf-8")
            if "luxury_lakehouse-" in text and WHEEL_FILENAME not in text:
                # Extract the stale version for the error message
                match = re.search(r"luxury_lakehouse-(\d+\.\d+\.\d+)-py3", text)
                version = match.group(1) if match else "unknown"
                stale.append(f"{py_file.name} (has {version})")

        assert not stale, (
            f"PEP 723 scripts reference stale wheel (expected {WHEEL_FILENAME}):\n"
            + "\n".join(f"  - {s}" for s in stale)
            + "\nRun: uv run python scripts/bump_wheel.py"
        )


class TestEmbeddedWorkerScripts:
    """Evolve and benchmark embedded worker scripts must reference current version."""

    def test_evolve_backend_url(self) -> None:
        from shared.wheel import WHEEL_BASE_URL

        hf_jobs_py = _PROJECT_ROOT / "src" / "evolve" / "backends" / "hf_jobs.py"
        text = hf_jobs_py.read_text(encoding="utf-8")
        assert WHEEL_BASE_URL in text, (
            f"src/evolve/backends/hf_jobs.py does not contain {WHEEL_BASE_URL}. "
            "The embedded worker script URL should use shared.wheel.WHEEL_BASE_URL."
        )

    def test_benchmark_worker_url(self) -> None:
        from shared.wheel import WHEEL_BASE_URL

        benchmark_py = _PROJECT_ROOT / "scripts" / "benchmark_hf_jobs.py"
        text = benchmark_py.read_text(encoding="utf-8")
        assert WHEEL_BASE_URL in text, (
            f"scripts/benchmark_hf_jobs.py does not contain {WHEEL_BASE_URL}. "
            "The embedded worker script URL should use shared.wheel.WHEEL_BASE_URL."
        )


class TestDeployScriptsVersion:
    """Deploy scripts must reference the current wheel version."""

    def test_deploy_sh(self) -> None:
        from shared.wheel import WHEEL_FILENAME

        deploy_sh = _PROJECT_ROOT / "scripts" / "deploy.sh"
        if not deploy_sh.exists():
            pytest.skip("deploy.sh not found (may be deprecated)")
        text = deploy_sh.read_text(encoding="utf-8")
        assert WHEEL_FILENAME in text, (
            f"scripts/deploy.sh references stale wheel (expected {WHEEL_FILENAME}). "
            "Run: uv run python scripts/bump_wheel.py"
        )

    def test_manage_space_uses_import(self) -> None:
        """manage_space.py should import WHEEL_FILENAME, not use a glob."""
        manage_py = _PROJECT_ROOT / "scripts" / "manage_space.py"
        text = manage_py.read_text(encoding="utf-8")
        assert "from shared.wheel import" in text, (
            "scripts/manage_space.py should import from shared.wheel "
            "for version-aware wheel selection"
        )

    def test_deploy_wheel_uses_import(self) -> None:
        """deploy_wheel.py should import WHEEL_FILENAME, not list HF Hub."""
        deploy_py = _PROJECT_ROOT / "scripts" / "deploy_wheel.py"
        text = deploy_py.read_text(encoding="utf-8")
        assert "from shared.wheel import" in text, (
            "scripts/deploy_wheel.py should import from shared.wheel "
            "instead of listing HF Hub files"
        )
```

- [ ] **Step 2: Run to confirm they are RED**

Run: `uv run pytest src/tests/test_wheel_conformance.py -v`
Expected: Multiple failures — stale PEP 723 scripts, missing imports in deploy scripts, evolve backend not yet updated

- [ ] **Step 3: Run lint**

Run: `uv run ruff check src/tests/test_wheel_conformance.py`
Expected: PASS

---

### Task 4: Convert dynamic consumers to import from `shared.wheel`

**Files:**
- Modify: `src/evolve/backends/hf_jobs.py:14,38-43`
- Modify: `scripts/benchmark_hf_jobs.py:21,70-76`
- Modify: `scripts/manage_space.py:22,169-181`
- Modify: `scripts/deploy_wheel.py:23,58-69`

#### 4a. Evolve backend — dynamic worker script URL

- [ ] **Step 1: Update `src/evolve/backends/hf_jobs.py`**

Add import at the top (after existing imports, ~line 25):
```python
from shared.wheel import WHEEL_BASE_URL
```

Replace the static `_WORKER_SCRIPT` string (lines 38-43). Change from:
```python
_WORKER_SCRIPT = '''\
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse[analytics,training] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
# ]
# ///
```

To:
```python
_WORKER_SCRIPT = f'''\
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse[analytics,training] @ {WHEEL_BASE_URL}",
# ]
# ///
```

- [ ] **Step 2: Run evolve tests**

Run: `uv run pytest src/tests/test_evolve_evaluator.py -v`
Expected: PASS

#### 4b. Benchmark script — dynamic worker script URL

- [ ] **Step 3: Update `scripts/benchmark_hf_jobs.py`**

Add import (after line 22):
```python
from shared.wheel import WHEEL_BASE_URL
```

Replace the embedded WORKER_SCRIPT (lines 70-76). Change from:
```python
WORKER_SCRIPT = '''\
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse[analytics,training] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
# ]
# ///
```

To:
```python
WORKER_SCRIPT = f'''\
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse[analytics,training] @ {WHEEL_BASE_URL}",
# ]
# ///
```

#### 4c. manage_space.py — version-aware wheel selection

- [ ] **Step 4: Update `scripts/manage_space.py`**

Add import (after line 29):
```python
from shared.wheel import WHEEL_FILENAME
```

Replace the glob-based wheel selection (lines 169-181). Change from:
```python
    # Find built wheel
    wheels = globmod.glob(str(dist_src / "luxury_lakehouse-*.whl"))
    if not wheels:
        msg = f"No wheel found in {dist_src} after uv build"
        raise SpaceError(msg)

    # Copy to hf_taipy_app/dist/
    if dist_dst.exists():
        shutil.rmtree(dist_dst)
    dist_dst.mkdir(parents=True)
    for whl in wheels:
        shutil.copy2(whl, dist_dst)
    logger.info("Bundled wheel: %s -> %s", [os.path.basename(w) for w in wheels], dist_dst)
    return dist_dst
```

To:
```python
    # Find built wheel (exact version match — avoids bundling stale wheels)
    expected_wheel = dist_src / WHEEL_FILENAME
    if not expected_wheel.exists():
        msg = f"Expected wheel not found: {expected_wheel}. Is pyproject.toml version in sync?"
        raise SpaceError(msg)

    # Copy to hf_taipy_app/dist/
    if dist_dst.exists():
        shutil.rmtree(dist_dst)
    dist_dst.mkdir(parents=True)
    shutil.copy2(str(expected_wheel), dist_dst)
    logger.info("Bundled wheel: %s -> %s", WHEEL_FILENAME, dist_dst)
    return dist_dst
```

#### 4d. deploy_wheel.py — use shared constants

- [ ] **Step 5: Update `scripts/deploy_wheel.py`**

Add import (after line 23):
```python
from shared.wheel import WHEEL_FILENAME
```

Delete the `_find_wheel_filename()` function (lines 58-69). Replace its usage in `_download_wheel()`. Change from:
```python
def _find_wheel_filename() -> str:
    """Find the latest luxury_lakehouse wheel filename in the HF Hub repo."""
    from huggingface_hub import HfApi

    api = HfApi()
    files = api.list_repo_files(repo_id=HF_REPO_ID, repo_type=HF_REPO_TYPE)
    wheels = [f for f in files if f.startswith("luxury_lakehouse") and f.endswith(".whl")]
    if not wheels:
        logger.error("No luxury_lakehouse wheel found in %s", HF_REPO_ID)
        sys.exit(1)
    # Sort by name (version ordering) and take the latest
    wheels.sort()
    return wheels[-1]


def _download_wheel() -> Path:
    """Download the wheel from HuggingFace Hub and return the local path."""
    filename = _find_wheel_filename()
```

To:
```python
def _download_wheel() -> Path:
    """Download the wheel from HuggingFace Hub and return the local path."""
    filename = WHEEL_FILENAME
```

(Rest of `_download_wheel` remains unchanged.)

- [ ] **Step 6: Run lint on all modified files**

Run: `uv run ruff check src/evolve/backends/hf_jobs.py scripts/benchmark_hf_jobs.py scripts/manage_space.py scripts/deploy_wheel.py`
Expected: PASS

- [ ] **Step 7: Run conformance tests — dynamic consumer tests should now pass**

Run: `uv run pytest src/tests/test_wheel_conformance.py::TestEmbeddedWorkerScripts -v && uv run pytest src/tests/test_wheel_conformance.py::TestDeployScriptsVersion::test_manage_space_uses_import src/tests/test_wheel_conformance.py::TestDeployScriptsVersion::test_deploy_wheel_uses_import -v`
Expected: 4 PASS (evolve, benchmark, manage_space import, deploy_wheel import)

---

### Task 5: Fix all static consumers — run `bump_wheel.py`

**Files:**
- Modify: 14× `scripts/*_hf.py` (0.1.0 → 0.3.0)
- Modify: `scripts/deploy.sh` (0.1.0 → 0.3.0)
- Modify: `terraform/modules/workflows/variables.tf` (description example 0.1.0 → 0.3.0)

- [ ] **Step 1: Run bump_wheel.py to update all static consumers**

Run: `uv run python scripts/bump_wheel.py`
Expected: Output listing ~16 updated files

- [ ] **Step 2: Verify all conformance tests pass**

Run: `uv run pytest src/tests/test_wheel_conformance.py -v`
Expected: All PASS

- [ ] **Step 3: Verify bump_wheel.py --check now passes**

Run: `uv run python scripts/bump_wheel.py --check`
Expected: Exit 0 — "All N static consumers reference luxury_lakehouse-0.3.0-py3-none-any.whl"

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest src/tests/ -v`
Expected: All PASS

- [ ] **Step 5: Run full lint + type check**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run lint-imports`
Expected: All PASS

---

### Task 6: CI workflow — version consistency check + stale wheel cleanup

**Files:**
- Modify: `.github/workflows/python-ci.yml:60-73`

- [ ] **Step 1: Add version consistency check to CI**

Add a step after "Build wheel" (line 58) and before "Upload wheel to HF Hub" (line 60):

```yaml
      - name: Check wheel version consistency
        run: uv run python scripts/bump_wheel.py --check
```

- [ ] **Step 2: Update the wheel upload step to include hash sidecar and cleanup**

Replace the current upload step (lines 60-73):

```yaml
      - name: Upload wheel to HF Hub
        if: github.ref == 'refs/heads/main' && github.event_name == 'push'
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
        run: |
          uv run pip install huggingface_hub
          uv run python -c "
          from huggingface_hub import HfApi
          import glob
          wheels = glob.glob('dist/luxury_lakehouse-*.whl')
          api = HfApi()
          for whl in wheels:
              api.upload_file(path_or_fileobj=whl, path_in_repo=whl.split('/')[-1], repo_id='luxury-lakehouse/build-artifacts', repo_type='model')
              print(f'Uploaded {whl}')
          "
```

With:

```yaml
      - name: Upload wheel to HF Hub
        if: github.ref == 'refs/heads/main' && github.event_name == 'push'
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
        run: |
          uv run python -c "
          import hashlib, json
          from pathlib import Path
          from huggingface_hub import HfApi
          from shared.wheel import WHEEL_FILENAME

          api = HfApi()
          repo_id = 'luxury-lakehouse/build-artifacts'
          repo_type = 'model'
          wheel_path = Path('dist') / WHEEL_FILENAME

          if not wheel_path.exists():
              raise FileNotFoundError(f'Expected {wheel_path} — is pyproject.toml version in sync with shared/wheel.py?')

          # Upload wheel
          api.upload_file(path_or_fileobj=str(wheel_path), path_in_repo=WHEEL_FILENAME, repo_id=repo_id, repo_type=repo_type)
          print(f'Uploaded {WHEEL_FILENAME}')

          # Compute and upload SHA-256 sidecar
          sha256 = hashlib.sha256(wheel_path.read_bytes()).hexdigest()
          manifest = json.dumps({'filename': WHEEL_FILENAME, 'sha256': sha256}, indent=2)
          api.upload_file(path_or_fileobj=manifest.encode(), path_in_repo='sha256sums.json', repo_id=repo_id, repo_type=repo_type, commit_message=f'Update SHA-256 for {WHEEL_FILENAME}')
          print(f'SHA-256: {sha256}')

          # Clean up old-version wheels (keep only current)
          all_files = api.list_repo_files(repo_id=repo_id, repo_type=repo_type)
          stale = [f for f in all_files if f.startswith('luxury_lakehouse-') and f.endswith('.whl') and f != WHEEL_FILENAME]
          for old in stale:
              api.delete_file(path_in_repo=old, repo_id=repo_id, repo_type=repo_type, commit_message=f'Remove stale wheel {old}')
              print(f'Deleted stale: {old}')
          "
```

- [ ] **Step 3: Run lint on CI workflow (YAML syntax)**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/python-ci.yml'))" && echo OK`
Expected: OK (valid YAML)

---

### Task 7: SEC3 — SHA-256 hash pinning in PEP 723 scripts

This task pins the SHA-256 hash of the CI-built wheel into PEP 723 URL fragments. It must run AFTER CI has uploaded the wheel (Task 6), because local builds produce different hashes than CI builds.

**Files:**
- Modify: 14× `scripts/*_hf.py` (add `#sha256=HASH` to URL)

**Workflow:**
1. CI builds and uploads the wheel + `sha256sums.json` (Task 6)
2. Developer downloads the hash: `uv run python -c "from huggingface_hub import hf_hub_download; import json; m = json.loads(open(hf_hub_download('luxury-lakehouse/build-artifacts', 'sha256sums.json', repo_type='model')).read()); print(m['sha256'])"`
3. Developer runs: `uv run python scripts/bump_wheel.py --pin-hash <SHA256>`
4. All PEP 723 URLs get `#sha256=<SHA256>` appended

- [ ] **Step 1: Write test for hash pinning in rewrite function**

The rewrite function already supports `sha256` parameter (tested in Task 1, `test_adds_sha256_fragment`). Verify the end-to-end flow:

```python
# Add to src/tests/test_wheel_constants.py, in TestRewriteWheelUrl class:

    def test_pin_hash_on_pep723_line(self) -> None:
        """Full PEP 723 line with extras gets hash appended."""
        from shared.wheel import rewrite_wheel_url

        line = '#     "luxury-lakehouse[analytics,training] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.0-py3-none-any.whl",'
        sha = "a" * 64
        result = rewrite_wheel_url(line, "0.3.0", sha256=sha)
        assert f"luxury_lakehouse-0.3.0-py3-none-any.whl#sha256={'a' * 64}" in result
        # Surrounding PEP 723 comment syntax preserved
        assert result.startswith('#     "luxury-lakehouse[analytics,training]')
        assert result.endswith('",')
```

- [ ] **Step 2: Run test**

Run: `uv run pytest src/tests/test_wheel_constants.py::TestRewriteWheelUrl::test_pin_hash_on_pep723_line -v`
Expected: PASS

- [ ] **Step 3: Pin hashes after CI uploads the wheel**

After CI has run on this branch and uploaded the wheel:

Run:
```bash
# Download the hash manifest from HF Hub
SHA=$(uv run python -c "
from huggingface_hub import hf_hub_download
import json
path = hf_hub_download('luxury-lakehouse/build-artifacts', 'sha256sums.json', repo_type='model')
print(json.loads(open(path).read())['sha256'])
")
echo "SHA-256: $SHA"

# Pin hash in all PEP 723 scripts
uv run python scripts/bump_wheel.py --pin-hash "$SHA"
```

Expected: ~14 PEP 723 scripts updated with `#sha256=...` in their wheel URLs

- [ ] **Step 4: Verify conformance tests still pass**

Run: `uv run pytest src/tests/test_wheel_conformance.py -v`
Expected: All PASS (hash fragment doesn't break version matching)

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest src/tests/ -v`
Expected: All PASS

---

### Self-Review Checklist

1. **Spec coverage:**
   - D53 version management: ✅ wheel.py constants, bump_wheel.py automation, all 16 stale refs fixed
   - SEC3 SHA-256 pinning: ✅ CI hash sidecar, `--pin-hash` mode, PEP 723 `#sha256=` fragments
   - Stale wheel cleanup on HF Hub: ✅ CI step deletes old versions
   - Dynamic consumers never go stale: ✅ evolve, benchmark, manage_space, deploy_wheel import from shared.wheel
   - CI verification: ✅ `bump_wheel.py --check` step

2. **Placeholder scan:** No TBD/TODO/placeholder found.

3. **Type consistency:** `WHEEL_VERSION`, `WHEEL_FILENAME`, `WHEEL_BASE_URL`, `WHEEL_REPO` — used consistently. `rewrite_wheel_url(text, version, sha256=None)` signature matches all call sites. `read_pyproject_version(project_root)` signature matches all call sites.
