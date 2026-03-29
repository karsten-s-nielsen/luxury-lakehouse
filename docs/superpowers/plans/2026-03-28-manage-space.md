# `manage_space.py` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Single script owning the full HF Space lifecycle (create/deploy/status/rebuild/teardown), replacing `deploy_taipy.py`.

**Architecture:** One file (`scripts/manage_space.py`) with argparse subcommands. Reuses deploy logic from `deploy_taipy.py`, adds create/teardown/status/rebuild. Polling loop shared between deploy and rebuild.

**Tech Stack:** Python 3.10, `huggingface_hub` (`HfApi`, `get_token`), `argparse`

---

### Task 1: Write `scripts/manage_space.py` — constants, helpers, polling

**Files:**
- Create: `scripts/manage_space.py`
- Delete: `scripts/deploy_taipy.py`

- [ ] **Step 1: Create `scripts/manage_space.py` with constants and helpers ported from `deploy_taipy.py`**

```python
#!/usr/bin/env python3
"""Manage HuggingFace Space lifecycle: create, deploy, status, rebuild, teardown.

Replaces ``deploy_taipy.py`` with full lifecycle management. Same script for
staging and production — only the target name differs.

Usage:
    python scripts/manage_space.py create staging
    python scripts/manage_space.py deploy staging [--dry-run] [--no-clean] [--no-wait]
    python scripts/manage_space.py status staging
    python scripts/manage_space.py rebuild staging
    python scripts/manage_space.py teardown staging

Requires:
    - ``HF_TOKEN`` environment variable or ``huggingface-cli login``
"""

from __future__ import annotations

import argparse
import fnmatch
import logging
import os
import shutil
import sys
import time
from pathlib import Path

from huggingface_hub import HfApi, get_token

logger = logging.getLogger(__name__)

FOLDER_PATH = Path("hf_taipy_app")

TARGETS: dict[str, str] = {
    "staging": "luxury-lakehouse/staging",
    "production": "luxury-lakehouse/soccer-analytics-app",
}

# Patterns to exclude from upload (matched against relative paths within FOLDER_PATH).
# .dockerignore only governs Docker COPY — upload_folder() needs its own exclusions.
IGNORE_PATTERNS: list[str] = [
    ".venv",
    ".venv/**",
    "**/__pycache__",
    "**/__pycache__/**",
    "**/*.pyc",
    "architecture.*",
    "src/test_render.py",
    "src/pages/widget_spacing_test.py",
]

# Secrets to configure on the Space.  Each entry maps the HF secret name to the
# local environment variable that supplies its value.
SECRETS: dict[str, str] = {
    "DATABRICKS_HOST": "DATABRICKS_HOST",
    "DATABRICKS_TOKEN": "DATABRICKS_TOKEN",
    "LAKEBASE_HOST": "LAKEBASE_HOST",
    "LAKEBASE_ENDPOINT_NAME": "LAKEBASE_ENDPOINT_NAME",
    "LAKEBASE_DATABASE": "LAKEBASE_DATABASE",
    "GOLD_SCHEMA": "GOLD_SCHEMA",
}

POLL_INTERVAL_S = 15
POLL_TIMEOUT_S = 600


class SpaceError(Exception):
    """Raised when a Space operation fails."""


def _list_local_files(folder: Path) -> list[tuple[str, int]]:
    """List all files under *folder*, returning ``(relative_path, size)`` pairs."""
    files: list[tuple[str, int]] = []
    for root, dirs, filenames in os.walk(folder):
        dirs[:] = [d for d in dirs if d not in (".venv", "__pycache__")]
        for f in filenames:
            if f.endswith(".pyc"):
                continue
            fpath = os.path.join(root, f)
            rel = os.path.relpath(fpath, folder).replace(os.sep, "/")
            files.append((rel, os.path.getsize(fpath)))
    return sorted(files)


def _matches_any(path: str, patterns: list[str]) -> bool:
    """Check if *path* matches any of the ignore patterns."""
    for pat in patterns:
        if fnmatch.fnmatch(path, pat):
            return True
        parts = path.split("/")
        for i in range(len(parts)):
            partial = "/".join(parts[: i + 1])
            if fnmatch.fnmatch(partial, pat):
                return True
    return False


def _bundle_workflow_cards() -> Path | None:
    """Copy workflow-cards/ into hf_taipy_app/ for deployment.  Returns dst path or None."""
    cards_src = Path(__file__).parent.parent / "workflow-cards"
    cards_dst = Path(__file__).parent.parent / "hf_taipy_app" / "workflow-cards"
    if cards_src.is_dir():
        if cards_dst.exists():
            shutil.rmtree(cards_dst)
        shutil.copytree(cards_src, cards_dst)
        logger.info("Bundled %d workflow cards", len(list(cards_dst.glob("*.yaml"))))
        return cards_dst
    logger.warning("No workflow-cards/ directory found at %s — skipping bundle", cards_src)
    return None


def _cleanup_workflow_cards(cards_dst: Path | None) -> None:
    """Remove bundled workflow-cards/ copy after deployment."""
    if cards_dst is not None and cards_dst.exists():
        shutil.rmtree(cards_dst)
        logger.info("Cleaned up bundled workflow-cards")


def _poll_until_running(repo_id: str, api: HfApi, timeout_s: int = POLL_TIMEOUT_S) -> None:
    """Poll ``get_space_runtime()`` every 15 s until RUNNING or timeout.

    Logs every stage transition.  Raises ``SpaceError`` on timeout or terminal
    failure stages (BUILD_ERROR, CONFIG_ERROR, RUNTIME_ERROR).
    """
    deadline = time.monotonic() + timeout_s
    prev_stage = None

    while time.monotonic() < deadline:
        runtime = api.get_space_runtime(repo_id)
        stage = str(runtime.stage)

        if stage != prev_stage:
            logger.info("Space %s stage: %s", repo_id, stage)
            prev_stage = stage

        if stage == "RUNNING":
            logger.info("Space %s is RUNNING", repo_id)
            return

        if stage in ("BUILD_ERROR", "CONFIG_ERROR", "RUNTIME_ERROR"):
            msg = f"Space {repo_id} entered terminal stage: {stage}"
            raise SpaceError(msg)

        remaining = int(deadline - time.monotonic())
        logger.info("  Waiting... (%d s remaining)", remaining)
        time.sleep(POLL_INTERVAL_S)

    msg = f"Space {repo_id} did not reach RUNNING within {timeout_s} s (last stage: {prev_stage})"
    raise SpaceError(msg)


def _require_force_for_production(target: str, force: bool) -> None:
    """Exit if *target* is production and ``--force`` was not supplied."""
    if target == "production" and not force:
        logger.error(
            "Refusing to run destructive operation on production without --force. "
            "This is a safety gate — add --force if you are certain."
        )
        sys.exit(1)
```

- [ ] **Step 2: Verify the file parses**

Run: `python -c "import ast; ast.parse(open('scripts/manage_space.py').read()); print('OK')"`
Expected: `OK`

---

### Task 2: Add `create` subcommand

**Files:**
- Modify: `scripts/manage_space.py`

- [ ] **Step 1: Add `_create_space` function**

Append after the helper functions:

```python
def _create_space(repo_id: str, target: str, api: HfApi, *, force: bool, skip_secrets: bool) -> None:
    """Create a Space from scratch: delete existing, create new, configure secrets."""
    _require_force_for_production(target, force)

    # Step 1: Clean slate
    logger.info("Deleting existing Space %s (if any)...", repo_id)
    api.delete_repo(repo_id, repo_type="space", missing_ok=True)
    logger.info("Deleted (or did not exist)")

    # Step 2: Create
    logger.info("Creating Space %s (sdk=docker)...", repo_id)
    url = api.create_repo(repo_id, repo_type="space", space_sdk="docker")
    logger.info("Created: %s", url)

    # Step 3: Secrets
    if skip_secrets:
        logger.info("Skipping secret configuration (--skip-secrets)")
    else:
        missing_required: list[str] = []
        for secret_name, env_var in SECRETS.items():
            value = os.environ.get(env_var)
            if value:
                api.add_space_secret(repo_id, key=secret_name, value=value)
                logger.info("Secret %s: set from $%s", secret_name, env_var)
            elif secret_name == "DATABRICKS_TOKEN":  # pragma: allowlist secret
                missing_required.append(secret_name)
                logger.warning(
                    "Secret %s: NOT SET — $%s is not in environment",
                    secret_name,
                    env_var,
                )
            else:
                missing_required.append(secret_name)
                logger.warning("Secret %s: NOT SET — $%s is not in environment", secret_name, env_var)

        if missing_required:
            logger.error(
                "Missing secrets: %s. Set them via environment variables and re-run, "
                "or set manually:\n"
                "  python scripts/manage_space.py create %s --skip-secrets\n"
                "  Then set each secret in the HF web UI at https://huggingface.co/spaces/%s/settings",
                ", ".join(missing_required),
                target,
                repo_id,
            )
            sys.exit(1)

    # Step 4: Verify
    info = api.space_info(repo_id)
    logger.info(
        "Verified: Space %s exists (stage: %s, last_modified: %s)",
        repo_id,
        info.runtime.stage if info.runtime else "unknown",
        info.last_modified,
    )
    logger.info("Space URL: https://huggingface.co/spaces/%s", repo_id)
```

- [ ] **Step 2: Verify parse**

Run: `python -c "import ast; ast.parse(open('scripts/manage_space.py').read()); print('OK')"`
Expected: `OK`

---

### Task 3: Add `deploy` subcommand (with `--dry-run`)

**Files:**
- Modify: `scripts/manage_space.py`

- [ ] **Step 1: Add `_preflight` function**

```python
def _preflight(folder: Path, repo_id: str, api: HfApi) -> None:
    """Run pre-flight checks.  Raises ``SpaceError`` on failure."""
    if not folder.is_dir():
        msg = f"Folder {folder} does not exist"
        raise SpaceError(msg)

    readme = folder / "README.md"
    if not readme.is_file():
        msg = f"README.md missing in {folder} — HF Spaces requires it"
        raise SpaceError(msg)

    if not get_token():
        msg = "No HuggingFace token. Set HF_TOKEN or run `huggingface-cli login`"
        raise SpaceError(msg)

    try:
        info = api.space_info(repo_id)
        logger.info(
            "Target: %s  (stage: %s, last modified: %s)",
            repo_id,
            info.runtime.stage if info.runtime else "unknown",
            info.last_modified,
        )
    except Exception as exc:
        msg = f"Space {repo_id} not accessible: {exc}"
        raise SpaceError(msg) from exc
```

- [ ] **Step 2: Add `_dry_run` function**

```python
def _dry_run(folder: Path, repo_id: str, api: HfApi) -> None:
    """Preview what would be uploaded and deleted without making changes."""
    cards_dst = _bundle_workflow_cards()
    try:
        all_local = _list_local_files(folder)
        upload_files = [(f, sz) for f, sz in all_local if not _matches_any(f, IGNORE_PATTERNS)]
        ignored_files = [f for f, _ in all_local if _matches_any(f, IGNORE_PATTERNS)]
        upload_names = {f for f, _ in upload_files}

        remote_files: set[str] = set()
        try:
            for item in api.list_repo_tree(repo_id, repo_type="space", recursive=True):
                if hasattr(item, "size"):
                    remote_files.add(item.rfilename)
        except Exception:
            logger.warning("Could not list remote files for %s", repo_id)

        to_delete = remote_files - upload_names - {".gitattributes"}

        print(f"\n{'=' * 70}")
        print(f"  DRY RUN — target: {repo_id}")
        print(f"{'=' * 70}")

        print(f"\n  Files to UPLOAD ({len(upload_files)}):")
        for f, sz in upload_files:
            marker = " [NEW]" if f not in remote_files else ""
            print(f"    {f:55s} {sz:>8,} B{marker}")

        if ignored_files:
            print(f"\n  Files IGNORED ({len(ignored_files)}):")
            for f in ignored_files:
                print(f"    {f}")

        if to_delete:
            print(f"\n  Stale files to REMOVE from remote ({len(to_delete)}):")
            for f in sorted(to_delete):
                print(f"    {f}")
        else:
            print("\n  No stale files to delete.")

        total_bytes = sum(sz for _, sz in upload_files)
        print(f"\n  Total upload: {total_bytes:,} bytes ({total_bytes / 1024:.1f} KB)")
        print()
    finally:
        _cleanup_workflow_cards(cards_dst)
```

- [ ] **Step 3: Add `_deploy` function**

```python
def _deploy(folder: Path, repo_id: str, api: HfApi, *, clean: bool, wait: bool) -> None:
    """Upload *folder* to *repo_id*, optionally poll until RUNNING."""
    info_before = api.space_info(repo_id)
    before_ts = info_before.last_modified

    logger.info("Uploading %s -> %s (clean=%s) ...", folder, repo_id, clean)

    delete_patterns = ["**"] if clean else None

    cards_dst = _bundle_workflow_cards()
    try:
        # Temporarily remove workflow-cards from .gitignore so upload_folder includes them.
        gitignore = folder / ".gitignore"
        gitignore_backup = gitignore.read_text() if gitignore.is_file() else None
        if gitignore_backup and "workflow-cards" in gitignore_backup:
            cleaned = "\n".join(
                line for line in gitignore_backup.splitlines() if "workflow-cards" not in line
            ).strip()
            gitignore.write_text(cleaned + "\n" if cleaned else "")

        try:
            commit_info = api.upload_folder(
                folder_path=str(folder),
                repo_id=repo_id,
                repo_type="space",
                ignore_patterns=IGNORE_PATTERNS,
                delete_patterns=delete_patterns,
                commit_message="Deploy Taipy app via scripts/manage_space.py",
            )
        finally:
            if gitignore_backup is not None:
                gitignore.write_text(gitignore_backup)
    finally:
        _cleanup_workflow_cards(cards_dst)

    # Post-upload verification
    info_after = api.space_info(repo_id)
    after_ts = info_after.last_modified

    if before_ts and after_ts and after_ts > before_ts:
        logger.info("VERIFIED: last_modified advanced  %s -> %s", before_ts, after_ts)
    elif before_ts == after_ts:
        logger.warning("No change detected — remote may already match local")
    else:
        logger.info("Upload completed.  last_modified: %s", after_ts)

    if commit_info:
        logger.info("Commit: %s", commit_info)

    count = 0
    for item in api.list_repo_tree(repo_id, repo_type="space", recursive=True):
        if hasattr(item, "size"):
            count += 1
    logger.info("Remote file count after deploy: %d", count)

    if wait:
        _poll_until_running(repo_id, api)


def _deploy_command(
    folder: Path, repo_id: str, api: HfApi, *, dry_run: bool, clean: bool, wait: bool
) -> None:
    """Entry point for the deploy subcommand."""
    _preflight(folder, repo_id, api)
    if dry_run:
        _dry_run(folder, repo_id, api)
    else:
        _deploy(folder, repo_id, api, clean=clean, wait=wait)
```

- [ ] **Step 4: Verify parse**

Run: `python -c "import ast; ast.parse(open('scripts/manage_space.py').read()); print('OK')"`
Expected: `OK`

---

### Task 4: Add `status`, `rebuild`, `teardown` subcommands

**Files:**
- Modify: `scripts/manage_space.py`

- [ ] **Step 1: Add `_status` function**

```python
def _status(repo_id: str, api: HfApi) -> None:
    """Print current Space state."""
    try:
        info = api.space_info(repo_id)
    except Exception:
        logger.error("Space %s not found or not accessible", repo_id)
        sys.exit(1)

    runtime = api.get_space_runtime(repo_id)

    print(f"\n{'=' * 70}")
    print(f"  Space: {repo_id}")
    print(f"{'=' * 70}")
    print(f"  URL:           https://huggingface.co/spaces/{repo_id}")
    print(f"  Stage:         {runtime.stage}")
    print(f"  Hardware:      {runtime.hardware}")
    print(f"  SDK:           {info.sdk}")
    print(f"  Last modified: {info.last_modified}")
    print(f"  Sleep time:    {runtime.sleep_time}")

    count = 0
    for item in api.list_repo_tree(repo_id, repo_type="space", recursive=True):
        if hasattr(item, "size"):
            count += 1
    print(f"  Remote files:  {count}")
    print()
```

- [ ] **Step 2: Add `_rebuild_space` function**

```python
def _rebuild_space(repo_id: str, api: HfApi) -> None:
    """Factory reboot and poll until RUNNING."""
    logger.info("Requesting factory reboot for %s ...", repo_id)
    runtime = api.restart_space(repo_id, factory_reboot=True)
    logger.info("Reboot requested (stage: %s)", runtime.stage)
    _poll_until_running(repo_id, api)
```

- [ ] **Step 3: Add `_teardown_space` function**

```python
def _teardown_space(repo_id: str, target: str, api: HfApi, *, force: bool) -> None:
    """Pause and delete a Space completely."""
    _require_force_for_production(target, force)

    # Graceful pause (swallow errors — Space may already be stopped/broken)
    try:
        api.pause_space(repo_id)
        logger.info("Paused %s", repo_id)
    except Exception:
        logger.info("Could not pause %s (may already be stopped) — continuing with delete", repo_id)

    logger.info("Deleting Space %s ...", repo_id)
    api.delete_repo(repo_id, repo_type="space", missing_ok=True)

    # Verify deletion
    try:
        api.space_info(repo_id)
        logger.error("Space %s still exists after delete_repo — unexpected", repo_id)
        sys.exit(1)
    except Exception:
        logger.info("Verified: Space %s has been deleted", repo_id)
```

- [ ] **Step 4: Verify parse**

Run: `python -c "import ast; ast.parse(open('scripts/manage_space.py').read()); print('OK')"`
Expected: `OK`

---

### Task 5: Add `main()` with argparse subcommands

**Files:**
- Modify: `scripts/manage_space.py`

- [ ] **Step 1: Add `main()` function**

```python
def main() -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Manage HuggingFace Space lifecycle",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- create ---
    p_create = subparsers.add_parser("create", help="Create a Space from scratch")
    p_create.add_argument("target", choices=sorted(TARGETS), help="Target HF Space")
    p_create.add_argument("--force", action="store_true", help="Required for production targets")
    p_create.add_argument("--skip-secrets", action="store_true", help="Skip secret configuration")

    # --- deploy ---
    p_deploy = subparsers.add_parser("deploy", help="Upload app to an existing Space")
    p_deploy.add_argument("target", choices=sorted(TARGETS), help="Target HF Space")
    p_deploy.add_argument("--dry-run", action="store_true", help="Preview changes without uploading")
    p_deploy.add_argument("--no-clean", action="store_true", help="Skip deletion of stale remote files")
    p_deploy.add_argument("--no-wait", action="store_true", help="Upload but don't poll for RUNNING")

    # --- status ---
    p_status = subparsers.add_parser("status", help="Show current Space state")
    p_status.add_argument("target", choices=sorted(TARGETS), help="Target HF Space")

    # --- rebuild ---
    p_rebuild = subparsers.add_parser("rebuild", help="Factory reboot without recreating")
    p_rebuild.add_argument("target", choices=sorted(TARGETS), help="Target HF Space")

    # --- teardown ---
    p_teardown = subparsers.add_parser("teardown", help="Delete a Space completely")
    p_teardown.add_argument("target", choices=sorted(TARGETS), help="Target HF Space")
    p_teardown.add_argument("--force", action="store_true", help="Required for production targets")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    repo_id = TARGETS[args.target]
    api = HfApi()

    try:
        if args.command == "create":
            _create_space(repo_id, args.target, api, force=args.force, skip_secrets=args.skip_secrets)
        elif args.command == "deploy":
            _deploy_command(
                FOLDER_PATH, repo_id, api, dry_run=args.dry_run, clean=not args.no_clean, wait=not args.no_wait
            )
        elif args.command == "status":
            _status(repo_id, api)
        elif args.command == "rebuild":
            _rebuild_space(repo_id, api)
        elif args.command == "teardown":
            _teardown_space(repo_id, args.target, api, force=args.force)
    except SpaceError as exc:
        logger.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run ruff + pyright on the new script**

Run: `uv run ruff check scripts/manage_space.py && uv run ruff format --check scripts/manage_space.py`
Expected: PASS (fix any violations)

Run: `uv run pyright scripts/manage_space.py`
Expected: PASS (fix any type errors)

- [ ] **Step 3: Verify help output**

Run: `python scripts/manage_space.py --help`
Expected: Shows all 5 subcommands

Run: `python scripts/manage_space.py create --help`
Expected: Shows `target`, `--force`, `--skip-secrets`

Run: `python scripts/manage_space.py deploy --help`
Expected: Shows `target`, `--dry-run`, `--no-clean`, `--no-wait`

---

### Task 6: Delete `deploy_taipy.py` and update references

**Files:**
- Delete: `scripts/deploy_taipy.py`
- Modify: `docs/c4/architecture.dsl` (rename `deploy_taipy.py` → `manage_space.py`)

- [ ] **Step 1: Delete `scripts/deploy_taipy.py`**

Run: `rm scripts/deploy_taipy.py`

- [ ] **Step 2: Update C4 architecture DSL**

In `docs/c4/architecture.dsl`, replace:

```
deployScript = container "deploy_taipy.py" "CLI tool: pre-flight checks, upload_folder with ignore/delete patterns, post-upload verification, dry-run mode" "Python, huggingface_hub"
```

with:

```
deployScript = container "manage_space.py" "CLI tool: full Space lifecycle — create, deploy, status, rebuild, teardown. Pre-flight checks, upload_folder with ignore/delete patterns, secret management, polling." "Python, huggingface_hub"
```

And replace:

```
developer -> deployScript "Runs deploy_taipy.py staging [--dry-run]" "CLI"
```

with:

```
developer -> deployScript "Runs manage_space.py {create|deploy|status|rebuild|teardown} staging" "CLI"
```

- [ ] **Step 3: Regenerate C4 HTML**

Run the C4 skill or: `python docs/c4/c4_assemble.py`

- [ ] **Step 4: Run full quality gates**

Run: `uv run ruff check scripts/ && uv run pyright scripts/manage_space.py`
Expected: PASS

---

### Task 7: Live test — teardown broken Spaces

This is a manual execution task, not a code change.

- [ ] **Step 1: Teardown `staging`**

Run: `python scripts/manage_space.py teardown staging`
Expected: Logs show delete (or "did not exist"), verification passes.

- [ ] **Step 2: Teardown `staging2`**

`staging2` is not in TARGETS, so use a one-liner:

Run: `python -c "from huggingface_hub import HfApi; api = HfApi(); api.delete_repo('luxury-lakehouse/staging2', repo_type='space', missing_ok=True); print('Done')"`
Expected: `Done`

- [ ] **Step 3: Verify both are gone**

Run: `python -c "from huggingface_hub import HfApi; api = HfApi(); [print(s.id) for s in api.list_spaces(author='luxury-lakehouse')]"`
Expected: Only `luxury-lakehouse/soccer-analytics-app` listed.

---

### Task 8: Live test — create + deploy staging

- [ ] **Step 1: Create staging Space**

Run: `python scripts/manage_space.py create staging`
Expected: Space created, secrets configured (or instructions printed for DATABRICKS_TOKEN if not in env). If DATABRICKS_TOKEN missing, set it via HF web UI, then re-run with `--skip-secrets` or set the env var.

- [ ] **Step 2: Check status**

Run: `python scripts/manage_space.py status staging`
Expected: Shows Space exists, stage is NO_APP_FILE (no code deployed yet).

- [ ] **Step 3: Deploy**

Run: `python scripts/manage_space.py deploy staging`
Expected: Upload completes, polling starts, stage transitions through BUILDING → RUNNING. Full success = exit code 0.

- [ ] **Step 4: Verify staging is live**

Open: `https://huggingface.co/spaces/luxury-lakehouse/staging`
Expected: Taipy app loads, pages are navigable.

---

### Task 9: Live test — rebuild and teardown cycle

- [ ] **Step 1: Rebuild**

Run: `python scripts/manage_space.py rebuild staging`
Expected: Factory reboot, polling shows BUILDING → RUNNING.

- [ ] **Step 2: Teardown**

Run: `python scripts/manage_space.py teardown staging`
Expected: Space deleted, verified gone.

- [ ] **Step 3: Full cycle — create + deploy again**

Run: `python scripts/manage_space.py create staging && python scripts/manage_space.py deploy staging`
Expected: Fresh Space, full deploy, reaches RUNNING. This proves repeatability.

---

### Task 10: Final status — leave staging running

- [ ] **Step 1: Verify staging RUNNING**

Run: `python scripts/manage_space.py status staging`
Expected: Stage = RUNNING, correct file count.

- [ ] **Step 2: Report to user for approval**

Present: script complete, all lifecycle commands tested, staging is live and verified. Ready for user to review and approve commit.
