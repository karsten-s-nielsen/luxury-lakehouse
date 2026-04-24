"""One-shot CLI to push HuggingFace dataset / model / Space README cards.

PR 4c's shared helper ``ingestion.hf_publish.upload_hf_readme`` covers the
auto-push case where a data publisher or training script uploads its README
alongside the payload. This script handles the remaining push paths:

  1. **Org Space** (``luxury-lakehouse/README``) — ``docs/huggingface/org-card.md``
     is not attached to any payload-producing publisher, so it needs its own
     push command. Replaces the previous manual web-UI paste step.

  2. **Orphan model cards** — five HF model repos document *methods* rather
     than trained weights (``pitch-control``, ``defcon``, ``off-ball-xt``,
     ``obso-pausa-method``, ``space-creation-method``) plus the
     ``football2vec-l2-harvest`` research repo. None of these have a
     training script that would push their card, so they're pushed here.

  3. **Any individual card by name** — escape hatch when someone updates a
     card and wants to push it immediately without running the whole
     producer pipeline.

Usage::

    # Push the org Space README (docs/huggingface/org-card.md).
    uv run python scripts/publish_hf_cards.py --org

    # Push every orphan model card in a single run.
    uv run python scripts/publish_hf_cards.py --orphans

    # Push a specific card by name + kind.
    uv run python scripts/publish_hf_cards.py --kind model --name pitch-control.md
    uv run python scripts/publish_hf_cards.py --kind dataset --name pining-for-the-data.md

Environment:
  HF_TOKEN   — write token for the ``luxury-lakehouse`` org. Required.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"

# Model repos documenting methods/heuristics + the football2vec L2 harvest —
# no training script publishes their cards, so this CLI is their push path.
# Each entry maps the HF repo basename to the in-repo card basename.
_ORPHAN_MODEL_CARDS: dict[str, str] = {
    "pitch-control": "pitch-control.md",
    "defcon": "defcon.md",
    "off-ball-xt": "off-ball-xt.md",
    "obso-pausa-method": "obso-pausa.md",
    "space-creation-method": "space-creation.md",
    "football2vec-l2-harvest": "football2vec-l2-harvest.md",
}


def _require_hf_token() -> str:
    token = os.environ.get("HF_TOKEN", "")
    if not token:
        msg = "HF_TOKEN environment variable required"
        raise RuntimeError(msg)
    return token


def _push_org_card(hf_token: str) -> None:
    """Push docs/huggingface/org-card.md to the luxury-lakehouse/README Space."""
    # The org card lives outside both dataset-cards/ and model-cards/, so
    # resolve its path directly rather than through get_hf_card_path.
    repo_root = Path(__file__).resolve().parent.parent
    card_path = repo_root / "docs" / "huggingface" / "org-card.md"

    result = upload_hf_readme(
        repo_id=f"{HF_ORG}/README",
        readme_path=card_path,
        hf_token=hf_token,
        repo_type="space",
    )
    logger.info(
        "Uploaded org-card: %s (sha256=%s)",
        result["commit_url"],
        result["sha256"][:8],
    )


def _push_model_card(repo_basename: str, card_name: str, hf_token: str) -> None:
    result = upload_hf_readme(
        repo_id=f"{HF_ORG}/{repo_basename}",
        readme_path=get_hf_card_path(card_name, kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    logger.info(
        "Uploaded model card to %s: %s (sha256=%s)",
        repo_basename,
        result["commit_url"],
        result["sha256"][:8],
    )


def _push_dataset_card(repo_basename: str, card_name: str, hf_token: str) -> None:
    result = upload_hf_readme(
        repo_id=f"{HF_ORG}/{repo_basename}",
        readme_path=get_hf_card_path(card_name, kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded dataset card to %s: %s (sha256=%s)",
        repo_basename,
        result["commit_url"],
        result["sha256"][:8],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--org",
        action="store_true",
        help="Push docs/huggingface/org-card.md to the luxury-lakehouse/README Space.",
    )
    group.add_argument(
        "--orphans",
        action="store_true",
        help=(
            "Push every orphan model card (methods + football2vec-l2-harvest) in one run. "
            "See module docstring for the full list."
        ),
    )
    group.add_argument(
        "--name",
        help=("Push a single card by basename (e.g. 'pitch-control.md'). Requires --kind."),
    )
    parser.add_argument(
        "--kind",
        choices=["dataset", "model"],
        help="Card type when using --name (ignored for --org and --orphans).",
    )
    parser.add_argument(
        "--repo",
        help=(
            "Override the HF repo basename when using --name. Defaults to the "
            "card basename minus '.md'. Use when the HF repo name differs from "
            "the card filename (e.g. obso-pausa.md card → obso-pausa-method repo)."
        ),
    )
    args = parser.parse_args()

    if args.name and not args.kind:
        parser.error("--name requires --kind")

    hf_token = _require_hf_token()

    if args.org:
        _push_org_card(hf_token)
        return

    if args.orphans:
        for repo_basename, card_name in _ORPHAN_MODEL_CARDS.items():
            _push_model_card(repo_basename, card_name, hf_token)
        return

    # --name path (argparse mutual-exclusion + parser.error guarantee these
    # are both set, but narrow explicitly for pyright).
    if args.name is None or args.kind is None:
        parser.error("--name requires --kind; one of --org / --orphans / --name must be set")
    repo_basename = args.repo or args.name.removesuffix(".md")
    if args.kind == "model":
        _push_model_card(repo_basename, args.name, hf_token)
    else:
        _push_dataset_card(repo_basename, args.name, hf_token)


if __name__ == "__main__":
    main()
