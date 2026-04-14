"""Directory-based workflow card loader and validation CLI.

Provides ``load_cards()`` for programmatic discovery and ``validate_cli()``
as a CLI entry point. Default directory is ``workflow-cards`` relative to
the current working directory, overridable with a positional argument.

Usage::

    uv run validate_workflow_cards                 # validates ./workflow-cards/
    uv run validate_workflow_cards <dir>           # validates <dir>
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from workflows.card import WorkflowCard

logger = logging.getLogger(__name__)


def load_cards(cards_dir: Path | str) -> dict[str, WorkflowCard]:
    """Scan *cards_dir* for ``*.yaml`` files and parse each as a :class:`WorkflowCard`.

    Returns a dict keyed by ``card.id``.  Non-YAML files are silently
    skipped.  An empty (or non-existent) directory returns ``{}``.
    """
    directory = Path(cards_dir)
    if not directory.is_dir():
        return {}

    cards: dict[str, WorkflowCard] = {}
    for yaml_path in sorted(directory.glob("*.yaml")):
        card = WorkflowCard.from_yaml_file(yaml_path)
        cards[card.id] = card
    return cards


def validate_cli() -> None:
    """CLI entry point: validate all YAML workflow cards in a directory.

    Usage::

        uv run validate_workflow_cards                 # validates ./workflow-cards/
        uv run validate_workflow_cards <dir>           # validates <dir>

    Exits with code 0 if every file is valid, code 1 if any file fails.
    """
    parser = argparse.ArgumentParser(description="Validate workflow card YAML files.")
    parser.add_argument(
        "directory",
        nargs="?",
        default="workflow-cards",
        metavar="DIR",
        help="Directory containing *.yaml workflow cards (default: ./workflow-cards/).",
    )
    args = parser.parse_args()

    directory = Path(args.directory)
    if not directory.is_dir():
        logger.error("%s is not a directory", directory)
        sys.exit(1)

    yaml_files = sorted(directory.glob("*.yaml"))
    has_errors = False

    for yaml_path in yaml_files:
        try:
            WorkflowCard.from_yaml_file(yaml_path)
            print(f"{yaml_path.name}: OK")
        except Exception as exc:
            has_errors = True
            print(f"{yaml_path.name}: ERROR — {exc}")

    if not yaml_files:
        print("No .yaml files found.")

    sys.exit(1 if has_errors else 0)


if __name__ == "__main__":
    validate_cli()
