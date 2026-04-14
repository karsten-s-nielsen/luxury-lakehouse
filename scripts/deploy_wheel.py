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
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from huggingface_hub import get_token, hf_hub_download

from shared.constants import IDENTIFIER_RE
from shared.wheel import WHEEL_FILENAME, WHEEL_REPO

logger = logging.getLogger(__name__)

HF_REPO_ID = WHEEL_REPO
HF_REPO_TYPE = "model"


def _validate_identifier(value: str, label: str) -> None:
    """Validate a catalog or schema name against SQL injection patterns."""
    if not IDENTIFIER_RE.match(value):
        logger.error("Invalid %s identifier: %r (must match %s)", label, value, IDENTIFIER_RE.pattern)
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


def _download_wheel() -> Path:
    """Download the wheel from HuggingFace Hub and return the local path."""
    filename = WHEEL_FILENAME
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
