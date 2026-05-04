"""HF4 invariant 1: no HF publishers or trainers in notebooks/ directory.

Per ADR-014 amendment (§4.2 of SK3-MIG-B spec): HF publishers and trainers
are PEP 723 scripts in scripts/. Notebook publishers and trainers are forbidden.

Scope: only notebooks/publish_*.py and notebooks/train_*.py are scanned.
Other notebooks (sync_hf_weights, import_obso_results, diag_*) are exempt.

Cleanest enforcement: "no notebooks/publish_*.py and no notebooks/train_*.py
exist post-HF4." The AST walk is belt-and-suspenders for any future
re-introduction.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"

# Forbidden call patterns — anything that uploads to HF Hub or registers MLflow models.
_FORBIDDEN_ATTRS = frozenset(
    {
        "upload_folder",
        "upload_file",
        "create_commit",
        "register_model",
        "set_registered_model_alias",
    }
)


def _scoped_files() -> list[Path]:
    """Files in scope: notebooks/publish_*.py + notebooks/train_*.py."""
    return sorted(list(NOTEBOOKS_DIR.glob("publish_*.py")) + list(NOTEBOOKS_DIR.glob("train_*.py")))


def test_no_publish_or_train_notebooks_exist() -> None:
    """Cleanest enforcement: post-HF4, these files MUST NOT exist."""
    files = _scoped_files()
    assert files == [], (
        f"Notebook HF publishers/trainers found (forbidden post-HF4): "
        f"{[str(f.relative_to(REPO_ROOT)) for f in files]}. "
        "Migrate to scripts/ as PEP 723 single-file scripts (see ADR-014 amendment)."
    )


@pytest.mark.parametrize("py_file", _scoped_files() or [None])
def test_ast_walk_finds_no_forbidden_calls(py_file: Path | None) -> None:
    """Belt-and-suspenders: even if the cleanest test passes vacuously
    (no files in scope), this AST walk catches any future re-introduction.

    Skips when no scoped files exist (parametrize edge case).
    """
    if py_file is None:
        pytest.skip("No notebooks/publish_*.py or notebooks/train_*.py files in scope")

    tree = ast.parse(py_file.read_text(encoding="utf-8"))

    forbidden_found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_ATTRS:
            forbidden_found.append(f"{py_file.name}:{node.lineno} -> .{node.attr}")
        # Also catch direct function imports: `from huggingface_hub import upload_file`
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in _FORBIDDEN_ATTRS:
                    forbidden_found.append(f"{py_file.name}:{node.lineno} -> import {alias.name}")

    assert not forbidden_found, (
        f"Forbidden HF/MLflow upload/registration calls in {py_file.relative_to(REPO_ROOT)}: "
        f"{forbidden_found}. Migrate to scripts/ as PEP 723 (see ADR-014 amendment)."
    )
