#!/usr/bin/env python3
"""Manage HuggingFace Space lifecycle: create, deploy, status, rebuild, teardown.

Replaces ``deploy_taipy.py`` with full lifecycle management. Same script for
staging and production -- only the target name differs.

Usage:
    python scripts/manage_space.py create staging [--force] [--skip-secrets]
    python scripts/manage_space.py deploy staging [--dry-run] [--no-clean] [--no-wait]
    python scripts/manage_space.py status staging
    python scripts/manage_space.py rebuild staging
    python scripts/manage_space.py teardown staging [--force]

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

from huggingface_hub import HfApi, SpaceHardware, get_token

logger = logging.getLogger(__name__)

FOLDER_PATH = Path("hf_taipy_app")

TARGETS: dict[str, str] = {
    "staging": "luxury-lakehouse/staging",
    "production": "luxury-lakehouse/soccer-analytics-app",
}

# Patterns to exclude from upload (matched against relative paths within FOLDER_PATH).
# .dockerignore only governs Docker COPY -- upload_folder() needs its own exclusions.
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

# Terminal stages that indicate the Space will not recover without intervention.
_TERMINAL_STAGES = frozenset({"BUILD_ERROR", "CONFIG_ERROR", "RUNTIME_ERROR"})


class SpaceError(Exception):
    """Raised when a Space operation fails."""


# ---------------------------------------------------------------------------
# Helper functions (ported from deploy_taipy.py)
# ---------------------------------------------------------------------------


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
        # Check each path prefix for directory-level matches.
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
    logger.warning("No workflow-cards/ directory found at %s -- skipping bundle", cards_src)
    return None


def _cleanup_workflow_cards(cards_dst: Path | None) -> None:
    """Remove bundled workflow-cards/ copy after deployment."""
    if cards_dst is not None and cards_dst.exists():
        shutil.rmtree(cards_dst)
        logger.info("Cleaned up bundled workflow-cards")


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------


def _poll_until_running(repo_id: str, api: HfApi, timeout_s: int = POLL_TIMEOUT_S) -> None:
    """Poll ``get_space_runtime()`` every 15 s until RUNNING or timeout.

    Logs every stage transition.  Raises ``SpaceError`` on timeout or terminal
    failure stages (BUILD_ERROR, CONFIG_ERROR, RUNTIME_ERROR).
    """
    deadline = time.monotonic() + timeout_s
    prev_stage: str | None = None

    while time.monotonic() < deadline:
        runtime = api.get_space_runtime(repo_id)
        stage = str(runtime.stage)

        if stage != prev_stage:
            logger.info("Space %s stage: %s", repo_id, stage)
            prev_stage = stage

        if stage == "RUNNING":
            logger.info("Space %s is RUNNING", repo_id)
            return

        if stage in _TERMINAL_STAGES:
            msg = f"Space {repo_id} entered terminal stage: {stage}"
            raise SpaceError(msg)

        remaining = max(0, int(deadline - time.monotonic()))
        if stage == prev_stage:
            logger.info("  Waiting... (%d s remaining)", remaining)
        time.sleep(POLL_INTERVAL_S)

    msg = f"Space {repo_id} did not reach RUNNING within {timeout_s} s (last stage: {prev_stage})"
    raise SpaceError(msg)


# ---------------------------------------------------------------------------
# Safety gates
# ---------------------------------------------------------------------------


def _require_force_for_production(target: str, force: bool) -> None:
    """Raise if *target* is production and ``--force`` was not supplied."""
    if target == "production" and not force:
        msg = (
            "Refusing to run destructive operation on production without --force. "
            "This is a safety gate -- add --force if you are certain."
        )
        raise SpaceError(msg)


# ---------------------------------------------------------------------------
# Subcommand: create
# ---------------------------------------------------------------------------


def _create_space(repo_id: str, target: str, api: HfApi, *, force: bool, skip_secrets: bool) -> None:
    """Create a Space from scratch: delete existing, create new, configure secrets."""
    _require_force_for_production(target, force)

    # Step 1: Clean slate
    logger.info("Deleting existing Space %s (if any)...", repo_id)
    api.delete_repo(repo_id, repo_type="space", missing_ok=True)
    logger.info("Deleted (or did not exist)")

    # Step 2: Create (space_hardware is required to initialize the runtime backend)
    logger.info("Creating Space %s (sdk=docker, hardware=cpu-basic)...", repo_id)
    url = api.create_repo(repo_id, repo_type="space", space_sdk="docker", space_hardware=SpaceHardware.CPU_BASIC)
    logger.info("Created: %s", url)

    # Step 3: Secrets
    if skip_secrets:
        logger.info("Skipping secret configuration (--skip-secrets)")
    else:
        missing_secrets: list[str] = []
        for secret_name, env_var in SECRETS.items():
            value = os.environ.get(env_var)
            if value:
                api.add_space_secret(repo_id, key=secret_name, value=value)
                logger.info("Secret %s: set from $%s", secret_name, env_var)
            else:
                missing_secrets.append(secret_name)
                logger.warning("Secret %s: NOT SET -- $%s is not in environment", secret_name, env_var)

        if missing_secrets:
            msg = (
                f"Missing secrets: {', '.join(missing_secrets)}. Set them via environment variables "
                f"and re-run, or set manually:\n"
                f"  python scripts/manage_space.py create {target} --skip-secrets\n"
                f"  Then set each secret in the HF web UI at https://huggingface.co/spaces/{repo_id}/settings"
            )
            raise SpaceError(msg)

    # Step 4: Verify
    info = api.space_info(repo_id)
    stage = info.runtime.stage if info.runtime else "unknown"
    logger.info("Verified: Space %s exists (stage: %s, last_modified: %s)", repo_id, stage, info.last_modified)
    logger.info("Space URL: https://huggingface.co/spaces/%s", repo_id)


# ---------------------------------------------------------------------------
# Subcommand: deploy
# ---------------------------------------------------------------------------


def _preflight(folder: Path, repo_id: str, api: HfApi) -> None:
    """Run pre-flight checks.  Raises ``SpaceError`` on failure."""
    if not folder.is_dir():
        msg = f"Folder {folder} does not exist"
        raise SpaceError(msg)

    readme = folder / "README.md"
    if not readme.is_file():
        msg = f"README.md missing in {folder} -- HF Spaces requires it"
        raise SpaceError(msg)

    if not get_token():
        msg = "No HuggingFace token. Set HF_TOKEN or run `huggingface-cli login`"
        raise SpaceError(msg)

    try:
        info = api.space_info(repo_id)
        stage = info.runtime.stage if info.runtime else "unknown"
        logger.info("Target: %s  (stage: %s, last modified: %s)", repo_id, stage, info.last_modified)
    except Exception as exc:
        msg = f"Space {repo_id} not accessible: {exc}"
        raise SpaceError(msg) from exc


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

        logger.info("\n%s\n  DRY RUN -- target: %s\n%s", "=" * 70, repo_id, "=" * 70)

        logger.info("  Files to UPLOAD (%d):", len(upload_files))
        for f, sz in upload_files:
            marker = " [NEW]" if f not in remote_files else ""
            logger.info("    %s %8s B%s", f"{f:55s}", f"{sz:,}", marker)

        if ignored_files:
            logger.info("  Files IGNORED (%d):", len(ignored_files))
            for f in ignored_files:
                logger.info("    %s", f)

        if to_delete:
            logger.info("  Stale files to REMOVE from remote (%d):", len(to_delete))
            for f in sorted(to_delete):
                logger.info("    %s", f)
        else:
            logger.info("  No stale files to delete.")

        total_bytes = sum(sz for _, sz in upload_files)
        logger.info("  Total upload: %s bytes (%.1f KB)", f"{total_bytes:,}", total_bytes / 1024)
    finally:
        _cleanup_workflow_cards(cards_dst)


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
            cleaned = "\n".join(line for line in gitignore_backup.splitlines() if "workflow-cards" not in line).strip()
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
            # Restore .gitignore
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
        logger.warning("No change detected -- remote may already match local")
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


def _deploy_command(folder: Path, repo_id: str, api: HfApi, *, dry_run: bool, clean: bool, wait: bool) -> None:
    """Entry point for the deploy subcommand."""
    _preflight(folder, repo_id, api)
    if dry_run:
        _dry_run(folder, repo_id, api)
    else:
        _deploy(folder, repo_id, api, clean=clean, wait=wait)


# ---------------------------------------------------------------------------
# Subcommand: status
# ---------------------------------------------------------------------------


def _status(repo_id: str, api: HfApi) -> None:
    """Print current Space state."""
    try:
        info = api.space_info(repo_id)
    except Exception as exc:
        msg = f"Space {repo_id} not found or not accessible: {exc}"
        raise SpaceError(msg) from exc

    runtime = api.get_space_runtime(repo_id)

    logger.info("\n%s\n  Space: %s\n%s", "=" * 70, repo_id, "=" * 70)
    logger.info("  URL:           https://huggingface.co/spaces/%s", repo_id)
    logger.info("  Stage:         %s", runtime.stage)
    logger.info("  Hardware:      %s", runtime.hardware)
    logger.info("  SDK:           %s", info.sdk)
    logger.info("  Last modified: %s", info.last_modified)
    logger.info("  Sleep time:    %s", runtime.sleep_time)

    count = 0
    for item in api.list_repo_tree(repo_id, repo_type="space", recursive=True):
        if hasattr(item, "size"):
            count += 1
    logger.info("  Remote files:  %d", count)


# ---------------------------------------------------------------------------
# Subcommand: rebuild
# ---------------------------------------------------------------------------


def _rebuild_space(repo_id: str, api: HfApi) -> None:
    """Factory reboot and poll until RUNNING."""
    logger.info("Requesting factory reboot for %s ...", repo_id)
    runtime = api.restart_space(repo_id, factory_reboot=True)
    logger.info("Reboot requested (stage: %s)", runtime.stage)
    _poll_until_running(repo_id, api)


# ---------------------------------------------------------------------------
# Subcommand: teardown
# ---------------------------------------------------------------------------


def _teardown_space(repo_id: str, target: str, api: HfApi, *, force: bool) -> None:
    """Pause and delete a Space completely."""
    _require_force_for_production(target, force)

    # Graceful pause (swallow errors -- Space may already be stopped/broken)
    try:
        api.pause_space(repo_id)
        logger.info("Paused %s", repo_id)
    except Exception:
        logger.info("Could not pause %s (may already be stopped) -- continuing with delete", repo_id)

    logger.info("Deleting Space %s ...", repo_id)
    api.delete_repo(repo_id, repo_type="space", missing_ok=True)

    # Verify deletion
    try:
        api.space_info(repo_id)
        msg = f"Space {repo_id} still exists after delete_repo -- unexpected"
        raise SpaceError(msg)
    except SpaceError:
        raise
    except Exception:
        logger.info("Verified: Space %s has been deleted", repo_id)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


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
