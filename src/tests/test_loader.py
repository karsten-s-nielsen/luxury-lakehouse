"""Tests for workflow card directory loader and validation CLI."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

from workflows.loader import load_cards

# ---------------------------------------------------------------------------
# Reusable YAML content
# ---------------------------------------------------------------------------

MINIMAL_YAML = textwrap.dedent("""\
    ---
    name: Test Workflow
    id: wf-test
    version: "1.0"
    status: production
    type: heuristic
    domain: testing
    owners:
      - test-user
    ---
    ## Overview
    Test workflow.
""")

SECOND_YAML = textwrap.dedent("""\
    ---
    name: Second Workflow
    id: wf-second
    version: "1.0"
    status: draft
    type: validation
    domain: testing
    owners:
      - another-user
    ---
    ## Overview
    Second workflow.
""")

INVALID_YAML = "not a valid workflow card at all"


# ---------------------------------------------------------------------------
# 1. load_cards discovers all .yaml files in directory
# ---------------------------------------------------------------------------


def test_load_cards_discovers_yaml_files(tmp_path: Path) -> None:
    (tmp_path / "card_a.yaml").write_text(MINIMAL_YAML, encoding="utf-8")
    (tmp_path / "card_b.yaml").write_text(SECOND_YAML, encoding="utf-8")

    cards = load_cards(tmp_path)
    assert len(cards) == 2
    assert "wf-test" in cards
    assert "wf-second" in cards


# ---------------------------------------------------------------------------
# 2. Returns dict keyed by card.id
# ---------------------------------------------------------------------------


def test_load_cards_keyed_by_card_id(tmp_path: Path) -> None:
    (tmp_path / "any_name.yaml").write_text(MINIMAL_YAML, encoding="utf-8")

    cards = load_cards(tmp_path)
    assert "wf-test" in cards
    assert cards["wf-test"].name == "Test Workflow"
    assert cards["wf-test"].domain == "testing"


# ---------------------------------------------------------------------------
# 3. Skips non-YAML files
# ---------------------------------------------------------------------------


def test_load_cards_skips_non_yaml_files(tmp_path: Path) -> None:
    (tmp_path / "card.yaml").write_text(MINIMAL_YAML, encoding="utf-8")
    (tmp_path / "readme.md").write_text("# Not a card", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("Some notes", encoding="utf-8")

    cards = load_cards(tmp_path)
    assert len(cards) == 1
    assert "wf-test" in cards


# ---------------------------------------------------------------------------
# 4. Handles empty directory gracefully
# ---------------------------------------------------------------------------


def test_load_cards_empty_directory(tmp_path: Path) -> None:
    cards = load_cards(tmp_path)
    assert cards == {}


# ---------------------------------------------------------------------------
# 5. validate_cli returns exit code 0 when all cards valid
# ---------------------------------------------------------------------------


def test_validate_cli_exit_0_all_valid(tmp_path: Path) -> None:
    (tmp_path / "card_a.yaml").write_text(MINIMAL_YAML, encoding="utf-8")
    (tmp_path / "card_b.yaml").write_text(SECOND_YAML, encoding="utf-8")

    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "workflows.loader", "--validate", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "OK" in result.stdout


# ---------------------------------------------------------------------------
# 6. validate_cli returns exit code 1 when a card has invalid YAML
# ---------------------------------------------------------------------------


def test_validate_cli_exit_1_invalid_card(tmp_path: Path) -> None:
    (tmp_path / "good.yaml").write_text(MINIMAL_YAML, encoding="utf-8")
    (tmp_path / "bad.yaml").write_text(INVALID_YAML, encoding="utf-8")

    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "workflows.loader", "--validate", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 1
    assert "ERROR" in result.stdout


# ---------------------------------------------------------------------------
# 7. load_cards accepts str path (not only Path)
# ---------------------------------------------------------------------------


def test_load_cards_accepts_str_path(tmp_path: Path) -> None:
    (tmp_path / "card.yaml").write_text(MINIMAL_YAML, encoding="utf-8")

    cards = load_cards(str(tmp_path))
    assert len(cards) == 1
    assert "wf-test" in cards


# ---------------------------------------------------------------------------
# 8. validate_cli on empty directory exits 0 (nothing to fail)
# ---------------------------------------------------------------------------


def test_validate_cli_empty_directory_exit_0(tmp_path: Path) -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "workflows.loader", "--validate", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
