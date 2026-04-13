"""Sync wheel version from pyproject.toml to all static consumer files.

Reads the canonical version from ``pyproject.toml`` and propagates it to
PEP 723 scripts, ``deploy.sh``, and Terraform files that embed the wheel
filename.

Usage::

    uv run python scripts/bump_wheel.py              # sync all files
    uv run python scripts/bump_wheel.py --check       # CI mode (exit 1 if stale)
    uv run python scripts/bump_wheel.py --dry-run     # preview changes
    uv run python scripts/bump_wheel.py --pin-hash SHA256  # add hash to URLs
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

from shared.wheel import (
    WHEEL_URL_RE,
    read_pyproject_version,
    rewrite_wheel_url,
    rewrite_wheel_version_constant,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Glob patterns for static consumer discovery
# ---------------------------------------------------------------------------
#
# Two groups based on whether the consumer can validate a #sha256= fragment:
#
# HASH_CONSUMERS — HTTP/HTTPS URLs (PEP 723 scripts, deploy.sh):
#     pip/uv validate the hash on download against the fetched bytes.
#     Receives both the version bump AND the #sha256= fragment.
#
# VERSION_ONLY_CONSUMERS — local file paths (Terraform → Databricks serverless):
#     Serverless pip rejects #sha256= on UC Volume paths (ERROR_INVALID_REQUIREMENT).
#     Receives ONLY the version bump, never the hash fragment.
#     Integrity for these wheels must be verified out-of-band (e.g., by
#     comparing the uploaded wheel's SHA-256 against sha256sums.json before
#     calling deploy_wheel.py).

_HASH_CONSUMER_GLOBS: list[str] = [
    "scripts/*.py",
    "scripts/*.sh",
]

_VERSION_ONLY_CONSUMER_GLOBS: list[str] = [
    "terraform/**/*.tf",
]

_SELF_NAME = "bump_wheel.py"
"""Exclude this script from consumer discovery."""


# ---------------------------------------------------------------------------
# Consumer discovery
# ---------------------------------------------------------------------------


def _discover_consumers_for_globs(project_root: Path, globs: list[str]) -> list[Path]:
    """Find static consumer files containing a wheel URL reference.

    Scans files matching the given globs and returns those whose content
    matches ``WHEEL_URL_RE``.  Excludes this script.
    """
    candidates: set[Path] = set()
    for pattern in globs:
        candidates.update(project_root.glob(pattern))

    # Exclude self
    candidates = {p for p in candidates if p.name != _SELF_NAME}

    consumers: list[Path] = []
    for path in sorted(candidates):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            logger.warning("Skipping unreadable file: %s", path)
            continue
        if WHEEL_URL_RE.search(text):
            consumers.append(path)

    return consumers


def _discover_hash_consumers(project_root: Path) -> list[Path]:
    """HTTP-URL consumers (PEP 723 scripts, deploy.sh) — receive ``#sha256=``."""
    return _discover_consumers_for_globs(project_root, _HASH_CONSUMER_GLOBS)


def _discover_version_only_consumers(project_root: Path) -> list[Path]:
    """Local-path consumers (Terraform) — version only, no ``#sha256=``."""
    return _discover_consumers_for_globs(project_root, _VERSION_ONLY_CONSUMER_GLOBS)


# ---------------------------------------------------------------------------
# Sync mode
# ---------------------------------------------------------------------------


def _sync(project_root: Path, *, dry_run: bool = False, sha256: str | None = None) -> int:
    """Propagate the pyproject.toml version to all consumer files.

    Returns the number of files that were (or would be) changed.
    """
    version = read_pyproject_version(project_root)
    changed = 0

    # 1. Update WHEEL_VERSION in src/shared/wheel.py
    wheel_module = project_root / "src" / "shared" / "wheel.py"
    if wheel_module.exists():
        original = wheel_module.read_text(encoding="utf-8")
        updated = rewrite_wheel_version_constant(original, version)
        if updated != original:
            changed += 1
            if dry_run:
                logger.info("Would update: %s", wheel_module.relative_to(project_root))
            else:
                wheel_module.write_text(updated, encoding="utf-8")
                logger.info("Updated: %s", wheel_module.relative_to(project_root))
        else:
            logger.debug("Already current: %s", wheel_module.relative_to(project_root))

    # 2. Update HTTP-URL consumers (PEP 723 scripts, deploy.sh) WITH hash
    for path in _discover_hash_consumers(project_root):
        original = path.read_text(encoding="utf-8")
        updated = rewrite_wheel_url(original, version, sha256=sha256)
        if updated != original:
            changed += 1
            if dry_run:
                logger.info("Would update (with hash): %s", path.relative_to(project_root))
            else:
                path.write_text(updated, encoding="utf-8")
                logger.info("Updated (with hash): %s", path.relative_to(project_root))
        else:
            logger.debug("Already current: %s", path.relative_to(project_root))

    # 3. Update local-path consumers (Terraform) — VERSION ONLY, never hash
    # Serverless pip rejects #sha256= on UC Volume paths.
    for path in _discover_version_only_consumers(project_root):
        original = path.read_text(encoding="utf-8")
        updated = rewrite_wheel_url(original, version, sha256=None)
        if updated != original:
            changed += 1
            if dry_run:
                logger.info("Would update (version only): %s", path.relative_to(project_root))
            else:
                path.write_text(updated, encoding="utf-8")
                logger.info("Updated (version only): %s", path.relative_to(project_root))
        else:
            logger.debug("Already current: %s", path.relative_to(project_root))

    if changed == 0:
        logger.info("All files already at version %s — nothing to do.", version)
    elif dry_run:
        logger.info("Dry run: %d file(s) would be updated to version %s.", changed, version)
    else:
        logger.info("Synced %d file(s) to version %s.", changed, version)

    return changed


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------


def _check(project_root: Path) -> int:
    """Verify that all consumer files reference the pyproject.toml version.

    Also verifies that local-path consumers (Terraform) do NOT contain a
    ``#sha256=`` fragment — serverless pip rejects it on UC Volume paths.

    Returns 0 if consistent, 1 if any file is stale or has a forbidden hash.
    """
    version = read_pyproject_version(project_root)
    stale: list[str] = []

    # Check WHEEL_VERSION constant — read from project_root's wheel.py file
    # (NOT the imported constant) so the function works correctly with test
    # fixtures that create a separate project tree.
    wheel_module = project_root / "src" / "shared" / "wheel.py"
    wheel_text = wheel_module.read_text(encoding="utf-8")
    wheel_match = re.search(r'WHEEL_VERSION\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"', wheel_text)
    actual_wheel_version = wheel_match.group(1) if wheel_match else "<not found>"
    if actual_wheel_version != version:
        stale.append(f"src/shared/wheel.py: WHEEL_VERSION={actual_wheel_version!r} (expected {version!r})")

    expected_filename = f"luxury_lakehouse-{version}-py3-none-any.whl"

    # Check HTTP-URL consumers — version match required, hash optional
    for path in _discover_hash_consumers(project_root):
        text = path.read_text(encoding="utf-8")
        for match in WHEEL_URL_RE.finditer(text):
            if expected_filename not in match.group(0):
                rel = path.relative_to(project_root)
                stale.append(f"{rel}: found {match.group(0)!r}")
                break

    # Check local-path consumers (Terraform) — version match required,
    # hash fragment FORBIDDEN
    for path in _discover_version_only_consumers(project_root):
        text = path.read_text(encoding="utf-8")
        for match in WHEEL_URL_RE.finditer(text):
            ref = match.group(0)
            if expected_filename not in ref:
                rel = path.relative_to(project_root)
                stale.append(f"{rel}: found {ref!r}")
                break
            if "#sha256=" in ref:
                rel = path.relative_to(project_root)
                stale.append(
                    f"{rel}: contains forbidden #sha256= fragment {ref!r} "
                    "(serverless pip rejects it on UC Volume paths)"
                )
                break

    if stale:
        logger.error("Wheel reference issues — expected %s:", version)
        for entry in stale:
            logger.error("  %s", entry)
        return 1

    logger.info("All files consistent at version %s.", version)
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for ``uv run python scripts/bump_wheel.py``."""
    parser = argparse.ArgumentParser(
        description="Sync wheel version from pyproject.toml to static consumer files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check consistency (CI mode). Exit 1 if any file is stale.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing files.",
    )
    parser.add_argument(
        "--pin-hash",
        metavar="SHA256",
        help="SHA-256 hash to append to PEP 723 wheel URLs (HTTP only). "
        "Terraform consumers (UC Volume paths) NEVER receive the hash — "
        "serverless pip rejects #sha256= on local file paths.",
    )
    args = parser.parse_args()

    if args.pin_hash and not re.fullmatch(r"[a-f0-9]{64}", args.pin_hash):
        parser.error("--pin-hash must be a 64-character lowercase hex string")

    logging.basicConfig(
        level=logging.DEBUG if args.dry_run else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    project_root = Path(__file__).resolve().parent.parent

    if args.check:
        sys.exit(_check(project_root))
    else:
        _sync(project_root, dry_run=args.dry_run, sha256=args.pin_hash)


if __name__ == "__main__":
    main()
