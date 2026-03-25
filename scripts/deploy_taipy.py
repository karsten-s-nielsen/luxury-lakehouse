#!/usr/bin/env python3
"""Deploy Taipy application to HuggingFace Spaces.

Uploads the ``hf_taipy_app/`` folder to the target HF Space with full sync:
files not present locally are deleted from the remote, ensuring the Space
always mirrors the local directory exactly.

Usage:
    python scripts/deploy_taipy.py staging --dry-run   # preview changes
    python scripts/deploy_taipy.py staging              # deploy to staging
    python scripts/deploy_taipy.py staging --no-clean   # upload without deleting stale files

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


def _preflight(folder: Path, repo_id: str, api: HfApi) -> None:
    """Run pre-flight checks.  Exits on failure."""
    if not folder.is_dir():
        logger.error("Folder %s does not exist", folder)
        sys.exit(1)

    readme = folder / "README.md"
    if not readme.is_file():
        logger.error("README.md missing in %s — HF Spaces requires it", folder)
        sys.exit(1)

    if not get_token():
        logger.error("No HuggingFace token. Set HF_TOKEN or run `huggingface-cli login`")
        sys.exit(1)

    try:
        info = api.space_info(repo_id)
        logger.info(
            "Target: %s  (stage: %s, last modified: %s)",
            repo_id,
            info.runtime.stage,
            info.last_modified,
        )
    except Exception:
        logger.exception("Space %s not accessible", repo_id)
        sys.exit(1)


def _dry_run(folder: Path, repo_id: str, api: HfApi) -> None:
    """Preview what would be uploaded and deleted without making changes."""
    cards_dst = _bundle_workflow_cards()
    all_local = _list_local_files(folder)
    upload_files = [(f, sz) for f, sz in all_local if not _matches_any(f, IGNORE_PATTERNS)]
    ignored_files = [f for f, _ in all_local if _matches_any(f, IGNORE_PATTERNS)]
    upload_names = {f for f, _ in upload_files}

    # Fetch remote file list.
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

    _cleanup_workflow_cards(cards_dst)


def _bundle_workflow_cards() -> Path | None:
    """Copy workflow-cards/ into hf_taipy_app/ for deployment. Returns dst path or None."""
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


def _deploy(folder: Path, repo_id: str, api: HfApi, *, clean: bool) -> None:
    """Upload *folder* to *repo_id* with optional stale-file cleanup."""
    info_before = api.space_info(repo_id)
    before_ts = info_before.last_modified

    logger.info("Uploading %s -> %s (clean=%s) ...", folder, repo_id, clean)

    delete_patterns = ["**"] if clean else None

    cards_dst = _bundle_workflow_cards()
    try:
        # workflow-cards/ is in .gitignore (prevents git commits of bundled
        # copies), but upload_folder respects .gitignore by default.
        # Temporarily remove the entry so cards are included in the upload.
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
                commit_message="Deploy Taipy app via scripts/deploy_taipy.py",
            )
        finally:
            # Restore .gitignore
            if gitignore_backup is not None:
                gitignore.write_text(gitignore_backup)
    finally:
        _cleanup_workflow_cards(cards_dst)

    # --- Post-upload verification ---
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

    # Count deployed files.
    count = 0
    for item in api.list_repo_tree(repo_id, repo_type="space", recursive=True):
        if hasattr(item, "size"):
            count += 1
    logger.info("Remote file count after deploy: %d", count)


def main() -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Deploy Taipy application to HuggingFace Spaces",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("target", choices=sorted(TARGETS), help="Target HF Space")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without uploading")
    parser.add_argument("--no-clean", action="store_true", help="Skip deletion of stale remote files")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    repo_id = TARGETS[args.target]
    api = HfApi()

    _preflight(FOLDER_PATH, repo_id, api)

    if args.dry_run:
        _dry_run(FOLDER_PATH, repo_id, api)
    else:
        _deploy(FOLDER_PATH, repo_id, api, clean=not args.no_clean)

    return 0


if __name__ == "__main__":
    sys.exit(main())
