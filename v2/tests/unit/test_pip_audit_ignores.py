"""The pip-audit wrapper: justified ignores only, no unexplained advisory, no stale ignore (spec §23.4)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

V2_ROOT = Path(__file__).resolve().parents[2]
IGNORES_FILE = V2_ROOT / "pip-audit-ignores.toml"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("v2_pip_audit", V2_ROOT / "scripts" / "pip_audit.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


pip_audit = _load_script()


def test_ignore_file_schema() -> None:
    ignores = pip_audit.load_ignores(IGNORES_FILE)
    assert ignores
    ids = [entry.id for entry in ignores]
    assert len(ids) == len(set(ids))
    for entry in ignores:
        for field in pip_audit.REQUIRED_FIELDS:
            assert str(getattr(entry, field)).strip(), f"{entry.id}: empty {field}"


def test_load_rejects_missing_field(tmp_path: Path) -> None:
    path = tmp_path / "ignores.toml"
    path.write_text('[[ignore]]\nid = "X-1"\npackage = "p 1.0"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="X-1"):
        pip_audit.load_ignores(path)


def test_load_rejects_duplicate_id(tmp_path: Path) -> None:
    entry = (
        '[[ignore]]\nid = "X-1"\npackage = "p 1.0"\nfix_in = "2.0"\nblocked_by = "b"\n'
        'justification = "j"\nreview_trigger = "r"\n'
    )
    path = tmp_path / "ignores.toml"
    path.write_text(entry + entry, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        pip_audit.load_ignores(path)


def _ignore(ident: str) -> object:
    return pip_audit.Ignore(
        id=ident, package="p 1.0", fix_in="2.0", blocked_by="b", justification="j", review_trigger="r"
    )


def test_evaluate_flags_unexplained_and_stale() -> None:
    reported = [
        pip_audit.Finding(package="a", version="1.0", id="A-1", aliases=()),
        pip_audit.Finding(package="b", version="1.0", id="B-1", aliases=("C-1",)),
    ]
    result = pip_audit.evaluate(reported, [_ignore("C-1"), _ignore("D-1")])
    assert [finding.id for finding in result.unexplained] == ["A-1"]
    assert [entry.id for entry in result.stale] == ["D-1"]


def test_evaluate_clean_when_every_finding_is_explained() -> None:
    reported = [pip_audit.Finding(package="a", version="1.0", id="A-1", aliases=())]
    result = pip_audit.evaluate(reported, [_ignore("A-1")])
    assert not result.unexplained
    assert not result.stale
