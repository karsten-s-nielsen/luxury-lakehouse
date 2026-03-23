#!/usr/bin/env python3
"""Deploy luxury-lakehouse wheel to Databricks UC Volume.

Downloads the pre-built wheel from HuggingFace Hub (``luxury-lakehouse/build-artifacts``)
and uploads it to a UC Volume so that Databricks jobs can install it at runtime.

Usage:
    python scripts/deploy_wheel.py --dry-run          # preview without uploading
    python scripts/deploy_wheel.py                    # deploy with defaults
    python scripts/deploy_wheel.py --catalog my_cat --schema my_schema
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from huggingface_hub import hf_hub_download
from huggingface_hub.utils import get_token

logger = logging.getLogger(__name__)

HF_REPO_ID = "luxury-lakehouse/build-artifacts"
HF_REPO_TYPE = "model"

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(value: str, label: str) -> None:
    """Validate a catalog or schema name against SQL injection patterns."""
    if not _IDENTIFIER_RE.match(value):
        logger.error("Invalid %s identifier: %r (must match %s)", label, value, _IDENTIFIER_RE.pattern)
        sys.exit(1)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Deploy luxury-lakehouse wheel to Databricks UC Volume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--catalog", default="soccer_analytics", help="Unity Catalog name (default: soccer_analytics)")
    parser.add_argument("--schema", default="bronze", help="Schema name (default: bronze)")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without uploading")
    return parser.parse_args()


def _preflight_hf() -> None:
    """Verify HuggingFace token is available."""
    if not get_token():
        logger.error("No HuggingFace token. Set HF_TOKEN or run `huggingface-cli login`")
        sys.exit(1)
    logger.info("HuggingFace token: present")


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
    logger.info("Downloading %s from %s ...", filename, HF_REPO_ID)
    local_path = hf_hub_download(
        repo_id=HF_REPO_ID,
        filename=filename,
        repo_type=HF_REPO_TYPE,
    )
    path = Path(local_path)
    size = path.stat().st_size
    logger.info("Downloaded: %s (%s bytes)", path, f"{size:,}")
    return path


def _upload_wheel(local_path: Path, volume_path: str, client: WorkspaceClient) -> None:
    """Upload the wheel to the UC Volume."""
    logger.info("Uploading %s -> %s ...", local_path, volume_path)
    with open(local_path, "rb") as f:
        client.files.upload(volume_path, f, overwrite=True)
    logger.info("Upload complete")


def _verify_upload(volume_path: str, local_path: Path, client: WorkspaceClient) -> None:
    """Verify the uploaded file exists and has the expected size."""
    logger.info("Verifying upload at %s ...", volume_path)
    status = client.files.get_metadata(volume_path)
    local_size = local_path.stat().st_size

    if status.content_length == local_size:
        logger.info("VERIFIED: remote size matches local (%s bytes)", f"{local_size:,}")
    elif status.content_length is not None:
        logger.warning(
            "Size mismatch: local=%s, remote=%s",
            f"{local_size:,}",
            f"{status.content_length:,}",
        )
    else:
        logger.info("Upload exists at %s (size not reported by API)", volume_path)


def main() -> None:
    """Entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args()

    # Validate identifiers against SQL injection.
    _validate_identifier(args.catalog, "catalog")
    _validate_identifier(args.schema, "schema")

    # Pre-flight: HF token.
    _preflight_hf()

    # Download wheel from HF Hub.
    local_path = _download_wheel()
    volume_path = f"/Volumes/{args.catalog}/{args.schema}/libs/{local_path.name}"

    if args.dry_run:
        logger.info("DRY RUN: would upload %s -> %s", local_path, volume_path)
        return

    # Upload to UC Volume.
    client = WorkspaceClient()
    _upload_wheel(local_path, volume_path, client)

    # Post-upload verification.
    _verify_upload(volume_path, local_path, client)

    logger.info("Wheel deployed: %s -> %s", local_path, volume_path)


if __name__ == "__main__":
    main()
