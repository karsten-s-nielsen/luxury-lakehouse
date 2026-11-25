"""No v2 calc-layer module may depend on a published HF dataset or model (spec D5; plan Task 2).

The owner's rule: nothing in v2 reads a previously-published HF artifact. import-linter already forbids the
compute/cards/publish packages from importing `huggingface_hub`; this adds the other half — no import of the HF
storage backend and no HF URL literal (`hf://…`, `huggingface.co/…`) — across the calc-layer packages, so a future
edit cannot quietly reintroduce the banned coupling. `test_the_gate_catches_a_violation` proves the scan bites on a
known-bad source, so the gate is red-proven before it is trusted green.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Final

SRC: Final = Path(__file__).resolve().parents[2] / "src" / "lakehouse"
_PACKAGES: Final = ("compute", "cards", "publish")  # the calc-layer packages (cards/publish land in later tasks)
_HF_URL: Final = re.compile(r"hf://|huggingface\.co")


def _is_hf_module(name: str) -> bool:
    return name == "huggingface_hub" or name.startswith("huggingface_hub.") or name == "lakehouse.storage.hf"


def hf_dependencies(source: str) -> list[str]:
    """The HF couplings in one module: an import of the HF client / HF storage backend, or an HF URL string literal."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found += [f"import {alias.name}" for alias in node.names if _is_hf_module(alias.name)]
        elif isinstance(node, ast.ImportFrom):
            if node.module and _is_hf_module(node.module):
                found.append(f"from {node.module}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and _HF_URL.search(node.value):
            found.append(f"hf url literal {node.value!r}")
    return found


def _scanned() -> list[Path]:
    paths: list[Path] = []
    for package in _PACKAGES:
        root = SRC / package
        if root.is_dir():
            paths += sorted(root.rglob("*.py"))
    return paths


def test_no_calc_layer_module_depends_on_hf() -> None:
    offenders = {
        path.relative_to(SRC).as_posix(): hits
        for path in _scanned()
        if (hits := hf_dependencies(path.read_text(encoding="utf-8")))
    }
    assert offenders == {}, f"calc-layer modules must not depend on a published HF artifact: {offenders}"


def test_the_gate_catches_a_violation() -> None:
    bad = 'import huggingface_hub\nURL = "hf://buckets/x"\nfrom lakehouse.storage.hf import HfStorage\n'
    hits = hf_dependencies(bad)
    assert any("huggingface_hub" in hit for hit in hits)
    assert any("hf url literal" in hit for hit in hits)
    assert any("lakehouse.storage.hf" in hit for hit in hits)


def test_the_scan_covers_real_modules() -> None:
    """A guard that scans nothing passes vacuously; the compute package and its base module must be in scope."""
    assert (SRC / "compute").is_dir()
    assert any(path.name == "base.py" for path in _scanned())
