"""New-private-import lint (silly-kicks 4.87.0 adoption, plan Task 15 / ADR-077).

Catch the NEXT accidental dependency on a silly-kicks *private* submodule at adoption
time. An underscore-prefixed module (``silly_kicks.<pkg>._<private>``) is not part of the
library's public API — depending on one couples us to internals that can move without a
semver signal. A small, deliberately-curated set of such dependencies exists and is
sanctioned (the xt-gk / ghost-gk trio the AC drain needs, also listed in
``ingestion.exec_visibility._SK_GUARD_SUBMODULES``). This test allows exactly those and
FAILS on any new private import that is not on the documented allowlist.

The scan is AST-based (``ast.parse`` + ``ast.walk``) — string literals, docstrings, and
``mock.patch("...")`` target strings that merely *name* a private path are NOT imports and
are correctly ignored; only real ``import`` / ``from ... import`` statements are flagged.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Documented, sanctioned private silly-kicks submodule dependencies.
#
# Seeded from the 4 underscore-private paths in
# ``src/ingestion/exec_visibility.py::_SK_GUARD_SUBMODULES`` (the executor version-skew
# guard). Every entry is a private *module* the repo intentionally imports; each needs a
# one-line justification. A new private import NOT listed here fails the test below.
#
# Beyond the four guard submodules, two single-symbol constants are imported by their
# private *name* off a public module path (``from silly_kicks.spadl.<p> import _NAME``) —
# these are the ``_PERIOD_START_SECONDS`` / ``_convert_locations`` uses the adoption plan
# (Task 15 Step 1) named as known-intentional; each is single-sourced from silly-kicks so
# the lakehouse and the library agree on a shared constant / conversion.
# ---------------------------------------------------------------------------
_ALLOWLIST: dict[str, str] = {
    "silly_kicks.tracking._ghost_gk": (
        "Ghost-GK positioning model (GKDV Layer 2). Sanctioned — listed in "
        "exec_visibility._SK_GUARD_SUBMODULES; imported by the executor version-skew guard."
    ),
    "silly_kicks.tracking._xt_gk": (
        "xT-GK value math (_gk_distribution_mask, _resolve_single_provider, "
        "_resolve_completion_for_frames). Sanctioned — in _SK_GUARD_SUBMODULES; used by the "
        "AC-drain xt-gk geometry resolution (enrich.py)."
    ),
    "silly_kicks.tracking._gk_completion": (
        "GK pass-completion (RAV) model. Sanctioned — in _SK_GUARD_SUBMODULES; part of the "
        "xt-gk trio the executor guard covers on the geometry-resolution path."
    ),
    "silly_kicks.tracking._gk_geometry": (
        "GK goal-kick coordinate resolver (native_origin_is_trusted, resolve_gk_geometry). "
        "Sanctioned — in _SK_GUARD_SUBMODULES; imported by action_context.enrich."
    ),
    "silly_kicks.spadl.skillcorner._PERIOD_START_SECONDS": (
        "Per-period start-offset constant that rebases SkillCorner frame timestamps onto the "
        "SPADL dispatch time base — single-sourced from silly-kicks so both agree (regression: "
        "test_skillcorner_dispatch_time_base.py / test_convert_drift.py). Sanctioned constant."
    ),
    "silly_kicks.spadl.statsbomb._convert_locations": (
        "StatsBomb coordinate y-flip + cell-centre conversion, imported verbatim so SB360 "
        "freeze frames and SPADL actions are byte-consistent (see sb360_freeze_frames.py). "
        "Sanctioned conversion helper."
    ),
}

_ROOTS = (REPO_ROOT / "src", REPO_ROOT / "scripts")


def _private_module_of(dotted: str) -> str | None:
    """Return the canonical private-module path for a dotted ``silly_kicks`` path, or
    ``None`` if the path is public.

    The canonical private module is the path truncated at (and including) the first
    single-underscore component after the ``silly_kicks`` root. Dunder components
    (``__version__``, ``__init__``) are treated as public and never truncate.
    """
    parts = dotted.split(".")
    if not parts or parts[0] != "silly_kicks":
        return None
    for i in range(1, len(parts)):
        name = parts[i]
        if name.startswith("_") and not name.startswith("__"):
            return ".".join(parts[: i + 1])
    return None


def _private_imports(tree: ast.AST) -> set[str]:
    """Collect canonical private ``silly_kicks`` module paths imported anywhere in a
    parsed module, across all three import shapes:

    * ``import silly_kicks.tracking._xt_gk``               (private component in the path)
    * ``from silly_kicks.tracking._xt_gk import X``        (private component in the path)
    * ``from silly_kicks.tracking import _ghost_gk``       (private NAME off a public path)
    """
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod = _private_module_of(alias.name)
                if mod is not None:
                    found.add(mod)
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative import — never silly_kicks
                continue
            module = node.module or ""
            if not module.startswith("silly_kicks"):
                continue
            mod = _private_module_of(module)
            if mod is not None:
                found.add(mod)
                continue
            # Public silly_kicks module path — flag a privately-named import off it
            # (the `from silly_kicks.tracking import _ghost_gk` submodule form).
            for alias in node.names:
                nm = alias.name
                if nm.startswith("_") and not nm.startswith("__"):
                    found.add(f"{module}.{nm}")
    return found


def _scan_paths(roots: Iterable[Path]) -> dict[str, list[str]]:
    """Return ``{private_module: [repo-relative locations]}`` across every ``.py`` file
    under ``roots``."""
    hits: dict[str, list[str]] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for py in sorted(root.rglob("*.py")):
            try:
                source = py.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            try:
                tree = ast.parse(source, filename=str(py))
            except SyntaxError:
                continue  # a syntax error elsewhere is not this lint's concern
            rel = py.relative_to(REPO_ROOT).as_posix()
            for mod in _private_imports(tree):
                hits.setdefault(mod, []).append(rel)
    return hits


# ---------------------------------------------------------------------------
# The gate.
# ---------------------------------------------------------------------------


def test_no_new_private_sk_imports() -> None:
    """No private silly-kicks import outside the documented allowlist."""
    hits = _scan_paths(_ROOTS)
    offenders = {mod: locs for mod, locs in hits.items() if mod not in _ALLOWLIST}
    assert not offenders, (
        "New private silly_kicks import(s) not on the documented allowlist in "
        f"{Path(__file__).name}:\n"
        + "\n".join(f"  - {mod}  (in {', '.join(sorted(set(locs)))})" for mod, locs in sorted(offenders.items()))
        + "\n\nIf this is an intentional, sanctioned dependency, add it to _ALLOWLIST with a "
        "one-line justification (cross-check ingestion.exec_visibility._SK_GUARD_SUBMODULES). "
        "Prefer a public silly_kicks API if one exists."
    )


# ---------------------------------------------------------------------------
# Non-vacuity proofs — a guard that can never fire is not a guard.
# ---------------------------------------------------------------------------


def test_allowlist_is_non_empty() -> None:
    """A guard with an empty allowlist would pass trivially only because nothing is
    allowed AND nothing is imported; assert the allowlist itself is populated."""
    assert _ALLOWLIST, "the sanctioned-private-import allowlist must not be empty"


_SYNTHETIC = (
    "import silly_kicks\n"
    "import silly_kicks.tracking as t\n"
    "from silly_kicks.tracking._made_up import thing\n"
    "import silly_kicks.tracking._other_private as o\n"
    "from silly_kicks.tracking import _submodule\n"
    "from silly_kicks import __version__\n"
    "from silly_kicks.tracking import features\n"
)


def test_scanner_detects_private_and_ignores_public() -> None:
    """Prove the AST walk is live and precise: it flags private modules in all three
    import shapes, and does NOT flag public silly_kicks imports, public names, or dunders.
    If a future edit breaks the walk (e.g. mishandles a node type), this fails."""
    found = _private_imports(ast.parse(_SYNTHETIC))
    assert found == {
        "silly_kicks.tracking._made_up",
        "silly_kicks.tracking._other_private",
        "silly_kicks.tracking._submodule",
    }, found


def test_scanner_is_wired_to_the_live_tree() -> None:
    """Prove the scanner actually reaches real source: the known-sanctioned private
    imports must be discovered in the live tree. If a refactor silently breaks the file
    walk, the scanner would match nothing and the gate would mute itself — this catches
    that by asserting the known imports are present."""
    hits = _scan_paths([REPO_ROOT / "src"])
    for known in (
        "silly_kicks.tracking._xt_gk",
        "silly_kicks.tracking._gk_geometry",
        "silly_kicks.tracking._ghost_gk",
    ):
        assert known in hits, (
            f"scanner did not find the known private import {known} in src/ — the AST walk "
            "may be broken or the sanctioned dependency was removed (update this list if so)."
        )
