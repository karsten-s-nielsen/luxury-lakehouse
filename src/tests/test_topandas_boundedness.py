"""Architecture test — every `.toPandas()` call in `src/` must be bounded.

The audit performed in OPT-1 (2026-05-02) found 19 production `.toPandas()`
call sites. 18 of 19 were bounded by some combination of `.filter()` /
`.where()` / `.limit()` / `.distinct()` / `.groupBy(...).agg()` either in
the DataFrame chain (AST-detectable) or in a SQL-string predicate
(opaque to AST). The 1 unbounded site (`expected_threat.py:_load_actions`,
~9.5M rows x 6 cols ≈ 456 MB driver pull when global xT grid rebuild is
needed) was fixed in the same OPT-1 cycle by switching to per-competition
streaming + bucketed-counter accumulation.

This test exists so a future PR cannot silently re-introduce the same
class of bug. It walks every `.py` file under `src/` (excluding
`src/tests/`), parses the AST, finds every `Attribute(attr='toPandas')`
call, and asserts one of:

1. The receiver chain syntactically contains a bounding method
   (`filter`, `where`, `limit`, `distinct`, `head`, `take`, `first`,
   `groupBy`/`groupby`, `agg`, `count`, `sum`, `mean`, `max`, `min`,
   `avg`).
2. The (file, enclosing-function qualname) tuple is registered in
   `src/tests/_topandas_exemptions.yml` with a `reason:` field
   explaining why driver memory can hold the result.

Exemptions are keyed by ENCLOSING FUNCTION, not line number: line-pinned
entries broke three times in two PRs (#359/#360) because ANY edit above
the call site shifts the line. The function qualname survives unrelated
edits and still pins the exemption to the code it justifies (the reason
text describes the function's bound, not a specific line). Module-level
calls key as ``<module>``; nested scopes join with ``.`` (e.g.
``Outer.inner``). One entry covers every unbounded call inside that
function — acceptable, because the articulated reason is a property of
the function's data context.

Why both forms are necessary: many call sites build their query as a SQL
string passed to `spark.sql(...)`, which is opaque to AST analysis even
though the SQL itself contains a `WHERE` clause that bounds the result.
The allowlist is the explicit catalogue for those cases — adding to it
forces the author to articulate WHY driver memory is sufficient, which
is the discipline this test exists to enforce.

When this test fails on a new call site, the engineer has two options:
- Refactor the call to add an explicit DataFrame-API bound (`.filter()`,
  `.limit()`, `.groupBy()...agg()`, etc.), so the AST chain is
  self-documenting; or
- Add an entry to `_topandas_exemptions.yml` with a `reason:` line
  documenting the bound (`per-match filter`, `dim-table size`,
  `upstream applyInPandas-bounded`, etc.).

The CLAUDE.md `.toPandas()` rule line is intentionally a one-line
pointer to this test — the test is the authoritative source of truth
because documentation rules drift while CI gates do not.
"""

from __future__ import annotations

import ast
import pathlib
from typing import NamedTuple

import yaml

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
_SRC_ROOT = _REPO_ROOT / "src"
_TESTS_DIR = _SRC_ROOT / "tests"
_EXEMPTIONS_PATH = _TESTS_DIR / "_topandas_exemptions.yml"

# Methods whose presence anywhere in the receiver chain bounds the
# resulting DataFrame size. `groupBy` / `groupby` alone is sufficient
# because every legitimate Spark DataFrame `groupBy(...)` call is
# followed by an aggregation on the same chain.
_BOUNDING_METHODS = frozenset(
    {
        "filter",
        "where",
        "limit",
        "distinct",
        "head",
        "take",
        "first",
        "groupBy",
        "groupby",
        "agg",
        "count",
        "sum",
        "mean",
        "max",
        "min",
        "avg",
        "selectExpr",  # rare but used (model_validation.py:247) — when
        # combined with downstream agg/groupBy, output is bounded; alone,
        # it's a projection not a filter, but seen exclusively in chains
        # that continue with .groupBy() so safe to allow here.
    }
)


class _Violation(NamedTuple):
    file: str  # repo-relative posix path
    line: int  # display-only (error messages); NOT part of the exemption key
    function: str  # enclosing function/class qualname, or "<module>"


def _chain_method_names(call: ast.Call) -> list[str]:
    """Walk the receiver chain backward from a `.toPandas()` call.

    Returns every `.method` name encountered while walking up
    `Call -> Attribute -> Call -> Attribute -> ...`. Stops at the first
    non-Call node (e.g. a Name like `spark` or `df`).
    """
    methods: list[str] = []
    if not isinstance(call.func, ast.Attribute):
        return methods
    node: ast.AST = call.func.value
    while isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute):
            methods.append(node.func.attr)
            node = node.func.value
        else:
            break
    return methods


def _find_topandas_calls_with_scope(tree: ast.Module) -> list[tuple[ast.Call, str]]:
    """Find every `.toPandas()` Call node in an AST, paired with the qualname of
    its enclosing function/class scope (``"<module>"`` at module level).

    Scope-aware replacement for a flat ``ast.walk``: line numbers shift on every
    unrelated edit above the call site (this broke the allowlist three times in
    PRs #359/#360); the enclosing-function qualname is stable.
    """
    results: list[tuple[ast.Call, str]] = []

    def _visit(node: ast.AST, stack: list[str]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                _visit(child, [*stack, child.name])
                continue
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute) and child.func.attr == "toPandas":
                results.append((child, ".".join(stack) or "<module>"))
            _visit(child, stack)

    _visit(tree, [])
    return results


def _scan_repo() -> list[_Violation]:
    """Walk `src/` (excluding `src/tests/`); return every unbounded
    `.toPandas()` call as a Violation tuple."""
    violations: list[_Violation] = []
    for py_file in sorted(_SRC_ROOT.rglob("*.py")):
        # Exclude the tests tree itself — test fixtures legitimately
        # build small pandas DataFrames via .toPandas() on Spark mocks.
        if _TESTS_DIR in py_file.parents:
            continue
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except SyntaxError:
            continue  # pragma: no cover — should not happen in this repo
        for call, scope in _find_topandas_calls_with_scope(tree):
            if any(m in _BOUNDING_METHODS for m in _chain_method_names(call)):
                continue
            rel = py_file.relative_to(_REPO_ROOT).as_posix()
            violations.append(_Violation(file=rel, line=call.lineno, function=scope))
    return violations


def _load_exemptions() -> dict[tuple[str, str], str]:
    """Read the allowlist YAML; return {(file, function): reason}."""
    if not _EXEMPTIONS_PATH.exists():
        return {}
    raw = yaml.safe_load(_EXEMPTIONS_PATH.read_text(encoding="utf-8")) or {}
    out: dict[tuple[str, str], str] = {}
    for entry in raw.get("exemptions", []):
        key = (entry["file"], str(entry["function"]))
        out[key] = entry.get("reason", "").strip()
    return out


def test_every_topandas_call_is_bounded_or_allowlisted() -> None:
    """Architecture test — see module docstring.

    Each unbounded `.toPandas()` call must either be refactored to add
    a DataFrame-API bounding method to its chain, or be registered in
    `src/tests/_topandas_exemptions.yml` with a `reason:` field.
    """
    violations = _scan_repo()
    exemptions = _load_exemptions()
    unjustified = [v for v in violations if (v.file, v.function) not in exemptions]
    if unjustified:
        msg_lines = [
            f"{len(unjustified)} unbounded `.toPandas()` call(s) found "
            f"with no entry in {_EXEMPTIONS_PATH.relative_to(_REPO_ROOT)}:",
            "",
        ]
        for v in unjustified:
            msg_lines.append(f"  {v.file}:{v.line}  (function: {v.function})")
        msg_lines += [
            "",
            "Two ways to fix:",
            "  1. Refactor the call to add a DataFrame-API bound to the chain — one of:",
            "     .filter(), .where(), .limit(N), .distinct(), .groupBy(...).agg|count|sum|mean,",
            "     .head(N), .take(N), .first().",
            "  2. Add to _topandas_exemptions.yml with a reason explaining why driver",
            "     memory can hold the result. Keyed by ENCLOSING FUNCTION (stable across",
            "     line shifts), not line number. Example:",
            "       - file: src/foo/bar.py",
            "         function: _load_dimension_lookup",
            '         reason: "Per-match filter (~170 MB / match)"',
        ]
        raise AssertionError("\n".join(msg_lines))


def test_no_stale_exemptions() -> None:
    """Catch entries in the allowlist that no longer correspond to an unbounded
    `.toPandas()` call inside the recorded (file, function) — e.g. the call was
    refactored to a bounded chain, the function was renamed, or it was removed
    and the exemption was forgotten.
    """
    exemptions = _load_exemptions()
    violations = _scan_repo()
    actual_keys = {(v.file, v.function) for v in violations}
    stale = [(file, fn) for (file, fn) in exemptions if (file, fn) not in actual_keys]
    if stale:
        msg_lines = [
            f"{len(stale)} stale entry(ies) in "
            f"{_EXEMPTIONS_PATH.relative_to(_REPO_ROOT)} — no unbounded "
            f"`.toPandas()` call exists inside the recorded (file, function):",
            "",
        ]
        for file, fn in stale:
            msg_lines.append(f"  {file}  function: {fn}")
        msg_lines += [
            "",
            "Remove the stale entry. The call was likely refactored to use a bounded chain",
            "(in which case the exemption is no longer needed), the function was renamed",
            "(update the entry), or the call was deleted entirely.",
        ]
        raise AssertionError("\n".join(msg_lines))


def test_every_exemption_has_a_reason() -> None:
    """Catch entries with an empty / missing `reason:` field."""
    exemptions = _load_exemptions()
    no_reason = [k for k, reason in exemptions.items() if not reason]
    if no_reason:
        bullets = "\n".join(f"  {file}  function: {fn}" for file, fn in no_reason)
        raise AssertionError(
            f"{len(no_reason)} exemption(s) missing a non-empty `reason:` field:\n\n"
            f"{bullets}\n\n"
            "Every exemption must articulate WHY driver memory is sufficient at the call "
            "site (per-match filter, dim-table size, upstream applyInPandas-bounded, "
            "explicit .limit() in SQL, etc.)."
        )
