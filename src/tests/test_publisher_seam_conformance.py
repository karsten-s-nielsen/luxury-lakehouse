"""ADR-072: the publish seam is the only door to a public HF repo.

Replaces the substring assertions retired across test_hf_publish_parity.py,
test_gradientsports_hf_exclusion.py, test_gradientsports_spadl.py,
test_publish_shot_freeze_frames.py and test_publish_xg_shot_data_v3.py. A substring check passes on
a mention in a comment, on a call against the RESTRICTED frame, and on a call placed AFTER the
upload — it cannot fail for the right reason. These are AST-based and derived from
PUBLISHER_REGISTRY, so a new publisher is covered the day it is added.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ingestion.hf_leak_guard import PUBLISHER_REGISTRY

# Repo-root anchored, never CWD-relative: src/tests/conftest.py does not chdir, so a glob relative
# to CWD returns [] when pytest runs from anywhere else — and a parametrized gate over [] collects
# zero cases and reports SUCCESS. Same idiom as test_hf_publish_parity.py.
_REPO_ROOT = Path(__file__).resolve().parents[2]

# The count assertion below is what makes "the seam module is outside the gate by construction"
# safe: widening the publish_*_hf.py glob changes this number and fails loudly with the file list,
# before either parametrized gate can silently go vacuous. Do not delete it as redundant.
_EXPECTED_PUBLISHER_FILE_COUNT = 15

# Every HfApi method capable of writing to (or enumerating) a repo, plus the seam's own private
# authorization hook. `upload_file` matters specifically: publish_shots_on_target_hf used it, so an
# `upload_folder`-only ban would have exempted the one publisher that had no guard at all.
# `_authorize` is the route that would defeat the runtime frame-authorization check.
_BANNED_ATTRIBUTE_CALLS: frozenset[str] = frozenset(
    {"upload_folder", "upload_file", "create_commit", "delete_files", "list_repo_files", "_authorize"}
)

# `GuardedFrame` construction and `dataclasses.replace` are the two forgery routes closed at runtime
# by the receipt's frame authorization. Banning them here is lint-time depth — a publisher has no
# reason to construct or re-shape a GuardedFrame itself.
#
# `replace` is matched precisely below, NOT as a bare attribute name: `dataclasses.replace(...)` is
# an ATTRIBUTE call, so adding "replace" to the attribute set would flag every `str.replace` /
# `df.replace` in the repo, and adding it to the name set alone would miss the dotted form.
_BANNED_NAME_CALLS: frozenset[str] = frozenset({"HfApi", "GuardedFrame", "replace"})


def _publisher_files() -> list[Path]:
    return sorted((_REPO_ROOT / "scripts").glob("publish_*_hf.py")) + sorted(
        (_REPO_ROOT / "src" / "ingestion").glob("publish_*_hf.py")
    )


def _call_names(tree: ast.AST) -> tuple[set[str], set[str]]:
    """(attribute-call names, bare-name call names) — e.g. ``api.upload_folder()`` vs ``HfApi()``."""
    attrs: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            attrs.add(node.func.attr)
        elif isinstance(node.func, ast.Name):
            names.add(node.func.id)
    return attrs, names


def _is_dataclasses_replace(node: ast.Call) -> bool:
    """``dataclasses.replace(...)`` — the dotted form the attribute ban deliberately does not cover."""
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "replace"
        and isinstance(func.value, ast.Name)
        and func.value.id == "dataclasses"
    )


def test_publisher_discovery_finds_every_file() -> None:
    found = _publisher_files()
    assert len(found) == _EXPECTED_PUBLISHER_FILE_COUNT, (
        f"expected {_EXPECTED_PUBLISHER_FILE_COUNT} publisher files, found {len(found)}: "
        f"{[str(p) for p in found]}. A short discovery makes the gates below vacuous."
    )


@pytest.mark.parametrize("path", _publisher_files(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_publisher_does_not_bypass_the_seam(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    attrs, names = _call_names(tree)
    banned = sorted((attrs & _BANNED_ATTRIBUTE_CALLS) | (names & _BANNED_NAME_CALLS))
    if any(isinstance(n, ast.Call) and _is_dataclasses_replace(n) for n in ast.walk(tree)):
        banned.append("dataclasses.replace")
    assert not banned, (
        f"{path.parent.name}/{path.name} bypasses the publish seam ({sorted(banned)}). Route uploads "
        f"through ingestion.hf_upload_seam.upload_guarded and obtain frames only from "
        f"prepare_public_upload / groupby / drop_columns — it is the only door (ADR-072). "
        f"The ADR-014 card push upload_hf_readme() is a bare function call and is unaffected; "
        f"get_token() is likewise not banned and is still needed."
    )


@pytest.mark.parametrize("path", _publisher_files(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_publisher_routes_through_the_seam(path: Path) -> None:
    attrs, names = _call_names(ast.parse(path.read_text(encoding="utf-8")))
    called = attrs | names
    assert "prepare_public_upload" in called, (
        f"{path.parent.name}/{path.name} never calls prepare_public_upload — its public frame is unguarded (ADR-072)."
    )
    assert "upload_guarded" in called, f"{path.parent.name}/{path.name} never calls upload_guarded (ADR-072)."


def test_registry_and_disk_agree() -> None:
    basenames = {p.stem for p in _publisher_files()}
    orphan_entries = set(PUBLISHER_REGISTRY) - basenames
    assert not orphan_entries, f"PUBLISHER_REGISTRY entries with no module on disk: {sorted(orphan_entries)}"
    unregistered = basenames - set(PUBLISHER_REGISTRY)
    assert not unregistered, f"publishers missing from PUBLISHER_REGISTRY: {sorted(unregistered)}"
