#!/usr/bin/env python3
"""Bump PINNED_DATASET_SHA in an evolve consumer script to HF Hub HEAD.

Operator-driven: requires --confirm-not-mid-experiment to prevent silent
data-shift during active architecture comparisons.

Usage:
    uv run python scripts/bump_evolve_pin.py scripts/evaluate_scoutgpt_l2_seeds.py \
        --confirm-not-mid-experiment \
        --reason "starting new architecture cycle XYZ"
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType


def _load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_bump_target", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    scripts_dir = str(path.parent)
    added = scripts_dir not in sys.path
    if added:
        sys.path.insert(0, scripts_dir)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    finally:
        if added and scripts_dir in sys.path:
            sys.path.remove(scripts_dir)
    return mod


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("script", type=Path, help="Path to evolve consumer script")
    parser.add_argument("--confirm-not-mid-experiment", action="store_true", required=True)
    parser.add_argument("--reason", type=str, required=True, help="Why the pin is being bumped")
    args = parser.parse_args()

    if not args.script.exists():
        raise FileNotFoundError(args.script)

    mod = _load(args.script)
    repo = getattr(mod, "PINNED_DATASET_REPO", None)
    if repo is None:
        raise ValueError(f"{args.script} has no PINNED_DATASET_REPO constant")

    from huggingface_hub import HfApi

    api = HfApi()
    info = api.dataset_info(repo_id=repo)
    new_sha = info.sha
    print(f"Repo: {repo}")
    print(f"HEAD SHA: {new_sha}")
    content = args.script.read_text(encoding="utf-8")
    content = re.sub(
        r'PINNED_DATASET_SHA:\s*str\s*=\s*"[^"]*"',
        f'PINNED_DATASET_SHA: str = "{new_sha}"',
        content,
    )
    content = re.sub(
        r'PINNED_REASON:\s*str\s*=\s*"[^"]*"',
        f'PINNED_REASON: str = "{args.reason}"',
        content,
    )
    args.script.write_text(content, encoding="utf-8")
    print(f"Updated {args.script}")


if __name__ == "__main__":
    main()
