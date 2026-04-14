#!/usr/bin/env python3
"""Idempotent HF Bucket provisioning — creates demo-data bucket and uploads parquet files.

Usage:
    python scripts/setup_hf_buckets.py [--data-dir demo_space/data]

Requires HF_TOKEN with write access to luxury-lakehouse org.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

BUCKET_ID = "luxury-lakehouse/demo-data"

DEMO_FILES = [
    "career_embeddings.parquet",
    "sample_shots.parquet",
    "sample_passes.parquet",
    "sample_tracking.parquet",
    "defcon_pressure.parquet",
    "sample_pausa.parquet",
]


def create_demo_bucket(api: HfApi) -> None:
    """Create the demo-data bucket if it doesn't exist."""
    logger.info("Creating bucket %s (exist_ok=True)", BUCKET_ID)
    api.create_bucket(BUCKET_ID, private=False, exist_ok=True)
    logger.info("Bucket %s ready", BUCKET_ID)


def upload_demo_data(api: HfApi, data_dir: Path) -> None:
    """Upload all demo parquet files to the bucket."""
    add_list: list[tuple[str | Path | bytes, str]] = []
    for name in DEMO_FILES:
        path = data_dir / name
        if path.exists():
            add_list.append((str(path), name))
            logger.info("Queued %s (%d bytes)", name, path.stat().st_size)
        else:
            logger.warning("Skipping %s — file not found at %s", name, path)

    if not add_list:
        logger.warning("No files to upload — data_dir may be empty")
        return

    logger.info("Uploading %d files to %s", len(add_list), BUCKET_ID)
    api.batch_bucket_files(bucket_id=BUCKET_ID, add=add_list)
    logger.info("Upload complete — %d files in %s", len(add_list), BUCKET_ID)


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Provision HF demo-data bucket")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("demo_space/data"),
        help="Directory containing demo parquet files (default: demo_space/data)",
    )
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        logger.error("Data directory not found: %s", args.data_dir)
        sys.exit(1)

    if not os.environ.get("HF_TOKEN"):
        logger.error("HF_TOKEN environment variable is not set — required for bucket write access")
        sys.exit(1)

    api = HfApi()
    create_demo_bucket(api)
    upload_demo_data(api, args.data_dir)


if __name__ == "__main__":
    main()
