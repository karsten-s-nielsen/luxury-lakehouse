"""§0 THE EVIDENCE INVARIANT — enforced by AST, not by a substring grep (P3 + V3).

> **The gate's EVIDENCE comes only from persisted tables, on an explicit allowlist:
> ``action_context_work_queue``, ``action_context_unit_events`` and ``spadl_action_context``
> (the cross-check). Its only task-value inputs are PARAMETERS (``run_id``, ``catalog``) —
> never evidence. Nothing from process memory.**

The rule exists because the same defect was introduced TWICE: a gate fed ``summary.timed_out``,
then one fed ``sink.write_failures`` — both in-memory objects living inside a *drain worker*, read
by a gate that runs in a **different task, in a different process**. And a Databricks **task value**
is the idiomatic way to smuggle exactly that across a task boundary, which is why it is clause 2.

WHY AST, AND WHY NAME RESOLUTION (V3 — the guard that could not fail)
---------------------------------------------------------------------
The first version of this guard collected string literals passed to ``spark.table(...)`` and
asserted ``literals ⊆ ALLOWED``. **The repo's house style contains no such literals** — the queue
port passes ``self._spark.table(self._table)``, an *attribute* assembled from an f-string over
module constants. A literal-collecting walker therefore gathers **∅**, ``∅ ⊆ ALLOWED`` always
holds, and the guard passes *no matter what the gate reads* — including a fourth table, i.e. the
very thing §0 forbids. The guard added to enforce §0 was itself an instance of the class §0 exists
to kill.

So this guard **forbids the SHAPES** rather than collecting the spellings:

1. every ``.table(...)`` argument must RESOLVE (through f-strings, module constants, local
   variables and ``self.<attr>`` assignments) to a table on the allowlist — and an argument that
   cannot be resolved is a **violation**, not a pass (fail-closed: an unresolvable read is an
   unauditable one);
2. ``.sql(...)`` and ``spark.read`` are forbidden outright — both bypass the allowlist entirely;
3. ``dbutils.jobs.taskValues.get(...)`` is allowed only for the PARAMETER keys;
4. importing ``analytics.action_context.drain`` (``DrainSummary``, the in-memory drain state) is
   forbidden — that is the original defect, by name.

Note that the gate DELEGATES its planner re-run to ``_ActionContextGuard().discover_units(...)``
rather than re-implementing the joins. That is deliberate (the diagnostic must dissent from the
*same* discovery the queue was built from), and it is what keeps every table name in the gate
module inside the allowlist — so a call into the planner must NOT read as a violation.

THE GUARD RULE (§0b): every guard must be shown to FAIL on a planted violation of the thing it
guards. ``test_invariant_guard_FAILS_on_planted_violations`` plants all THREE shapes the guard
claims to catch, and ``test_the_guard_actually_SEES_the_gates_table_reads`` proves the resolver is
not silently gathering ∅ on the real module. An invariant guard that has never failed is not a
guard.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import ingestion.action_context_gate as _gate_module
from ingestion.action_context import _TABLE_NAME as _RESULTS_TABLE
from ingestion.action_context_queue import _EVENT_TABLE, _QUEUE_TABLE

#: The allowlist, from the MODULE CONSTANTS — never re-spelled as literals here. ``_EVENT_TABLE``
#: is the UNION ALL *view*; the per-worker physical tables (``..._w0`` … ``_sb360``) are the sink's
#: business and must never leak into the gate.
_ALLOWED_TABLES = frozenset({_QUEUE_TABLE, _EVENT_TABLE, _RESULTS_TABLE})

#: Task values are PARAMETERS, never evidence.
_ALLOWED_TASK_VALUES = frozenset({"run_id", "catalog"})

#: In-memory drain state (``DrainSummary``, the live ``UnitEventSink``) lives here.
_FORBIDDEN_IMPORT = "analytics.action_context.drain"

_GATE_PATH = Path(_gate_module.__file__)
_MAX_DEPTH = 12


def _dotted(node: ast.expr) -> str:
    """``dbutils.jobs.taskValues.get`` from the Attribute chain; ``""`` if not a plain chain."""
    parts: list[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    else:
        return ""
    return ".".join(reversed(parts))


class _TableResolver:
    """Resolves a ``.table(...)`` argument to the TABLE NAME it names, or ``None``.

    Handles the three shapes that actually occur in this repo, none of which is a bare literal at
    the call site: an f-string over module constants (``f"{catalog}.{_QUEUE_SCHEMA}.{_QUEUE_TABLE}"``),
    a name bound to one, and an instance attribute assigned one (``self._table``). ``env`` is the
    *imported* module namespace, so constants pulled in via ``from ... import _EVENT_TABLE`` resolve
    to their real values rather than being written out again here.
    """

    def __init__(self, tree: ast.AST, env: Mapping[str, Any]) -> None:
        self._env = env
        self._names: dict[str, ast.expr] = {}
        self._attrs: dict[str, ast.expr] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self._names.setdefault(target.id, node.value)
                elif isinstance(target, ast.Attribute):
                    self._attrs.setdefault(target.attr, node.value)

    def resolve(self, node: ast.expr, depth: int = 0) -> str | None:
        if depth > _MAX_DEPTH:
            return None
        if isinstance(node, ast.Constant):
            return node.value.rsplit(".", 1)[-1] if isinstance(node.value, str) else None
        if isinstance(node, ast.JoinedStr):
            # The table name is the LAST component of the qualified name.
            return self.resolve(node.values[-1], depth + 1) if node.values else None
        if isinstance(node, ast.FormattedValue):
            return self.resolve(node.value, depth + 1)
        if isinstance(node, ast.Name):
            if node.id in self._names:
                return self.resolve(self._names[node.id], depth + 1)
            value = self._env.get(node.id)
            return value.rsplit(".", 1)[-1] if isinstance(value, str) else None
        if isinstance(node, ast.Attribute):
            if node.attr in self._attrs:
                return self.resolve(self._attrs[node.attr], depth + 1)
            return None
        return None


def gate_table_reads(src: str, env: Mapping[str, Any]) -> list[str | None]:
    """Every table the source reads, resolved. ``None`` = an unauditable (unresolvable) read.

    Exported so the tests can assert the resolver is NOT gathering ∅ on the real gate — which is
    exactly how the previous version of this guard managed to be vacuous.
    """
    tree = ast.parse(src)
    resolver = _TableResolver(tree, env)
    reads: list[str | None] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "table":
            reads.append(resolver.resolve(node.args[0]) if node.args else None)
    return reads


def gate_violations(src: str, env: Mapping[str, Any] | None = None) -> list[str]:
    """Every §0 violation in ``src``. FORBID THE SHAPES — do not collect the spellings."""
    env = {} if env is None else env
    tree = ast.parse(src)
    resolver = _TableResolver(tree, env)
    bad: list[str] = []

    for node in ast.walk(tree):
        # ── clause 4: in-memory drain state must not be importable into the gate ──
        if isinstance(node, ast.ImportFrom) and (node.module or "") == _FORBIDDEN_IMPORT:
            bad.append(
                f"imports in-memory drain state from {_FORBIDDEN_IMPORT!r} "
                f"({', '.join(a.name for a in node.names)}) — evidence must come from persisted tables"
            )
        if isinstance(node, ast.Import) and any(a.name == _FORBIDDEN_IMPORT for a in node.names):
            bad.append(f"imports the in-memory drain module {_FORBIDDEN_IMPORT!r}")

        # ── clause 2b: spark.read bypasses the table allowlist entirely ──
        if isinstance(node, ast.Attribute) and node.attr == "read" and _dotted(node).startswith("spark."):
            bad.append("uses `spark.read` — a path/format read bypasses the table allowlist")

        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        func = node.func
        dotted = _dotted(func)

        # ── clause 1: every .table(...) argument must resolve to an allowlisted table ──
        if func.attr == "table":
            resolved = resolver.resolve(node.args[0]) if node.args else None
            if resolved is None:
                bad.append(
                    f"`{dotted or '<expr>'}.table(...)` argument does not resolve to a table name — "
                    "an unresolvable read is an unauditable one (fail-closed)"
                )
            elif resolved not in _ALLOWED_TABLES:
                bad.append(f"reads NON-allowlisted table {resolved!r} (allowed: {sorted(_ALLOWED_TABLES)})")

        # ── clause 2a: raw SQL bypasses the allowlist ──
        elif func.attr == "sql":
            bad.append("uses raw `.sql(...)` — raw SQL bypasses the table allowlist")

        # ── clause 3: task values are PARAMETERS, never evidence ──
        elif dotted.endswith("taskValues.get"):
            keys = [kw.value for kw in node.keywords if kw.arg == "key"]
            key = keys[0] if keys else None
            if not (isinstance(key, ast.Constant) and key.value in _ALLOWED_TASK_VALUES):
                shown = ast.unparse(key) if key is not None else "<no key=>"
                bad.append(
                    f"reads task value key={shown} — task values are PARAMETERS "
                    f"(allowed: {sorted(_ALLOWED_TASK_VALUES)}), never EVIDENCE"
                )
    return bad


def _gate_src() -> str:
    return _GATE_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# The invariant itself
# ---------------------------------------------------------------------------


def test_gate_evidence_comes_only_from_allowlisted_tables() -> None:
    """§0: three persisted tables, two parameters, nothing from process memory."""
    assert gate_violations(_gate_src(), vars(_gate_module)) == []


def test_the_guard_actually_SEES_the_gates_table_reads() -> None:  # noqa: N802
    """ANTI-VACUITY (V3). The previous guard passed because it resolved NOTHING.

    ``∅ ⊆ ALLOWED`` is always true, so "no violations" is worthless unless the resolver demonstrably
    resolved the reads that ARE there. Pin them: exactly three ``.table(...)`` reads, all resolved,
    and together they are exactly the allowlist.
    """
    reads = gate_table_reads(_gate_src(), vars(_gate_module))
    resolved = [r for r in reads if r is not None]
    assert len(resolved) == len(reads), f"resolver failed to resolve a table read: {reads}"
    assert len(reads) == 3, f"expected 3 table reads in the gate (queue, events, results mart); saw {reads}"
    assert set(resolved) == set(_ALLOWED_TABLES), f"{sorted(set(resolved))} != {sorted(_ALLOWED_TABLES)}"


def test_delegating_to_the_planner_is_NOT_a_violation() -> None:  # noqa: N802
    """The gate re-runs the planner via ``_ActionContextGuard().discover_units(...)`` instead of
    re-implementing its joins — which is precisely what keeps its table names inside the allowlist.
    A guard that flagged the delegation would push the next implementer into re-implementing the
    joins here, i.e. into the fourth-table violation.
    """
    src = _gate_src()
    assert "discover_units" in src, "the gate no longer delegates to the planner — re-check clause 1"
    assert gate_violations(src, vars(_gate_module)) == []


# ---------------------------------------------------------------------------
# THE GUARD RULE (§0b) — the guard must be shown to FAIL
# ---------------------------------------------------------------------------

# (a) a FOURTH table, read through an ATTRIBUTE — the house style that made the literal-collecting
#     walker vacuous (V3). No string literal appears at the call site.
_PLANTED_TABLE = (
    "class G:\n"
    "    def __init__(self, spark, catalog):\n"
    "        self._spark = spark\n"
    "        self._t = f'{catalog}.gold.fct_action_context'\n"
    "    def run(self):\n"
    "        return self._spark.table(self._t)\n"
)

# (b) EVIDENCE pulled from a Databricks task value — the idiomatic way to smuggle in-memory state
#     across a task boundary, and therefore where instance #4 will come from.
_PLANTED_TASK_VALUE = (
    "def run(dbutils):\n    n = dbutils.jobs.taskValues.get(taskKey='compute', key='failed_units')\n    return n\n"
)

# (c) an import of the in-memory drain state — the ORIGINAL defect, twice (W6: the guard has three
#     clauses; planting two of them is §0b applied to two-thirds of a guard).
_PLANTED_IMPORT = (
    "from analytics.action_context.drain import DrainSummary\ndef run(s: DrainSummary):\n    return s.timed_out\n"
)

# (d) raw SQL — the fourth shape the guard claims, and the easiest way to read anything at all.
_PLANTED_SQL = "def run(spark):\n    return spark.sql('SELECT * FROM soccer_analytics.gold.fct_action_context')\n"


def test_invariant_guard_FAILS_on_planted_violations() -> None:  # noqa: N802
    """THE GUARD RULE (§0b). An invariant guard that has never failed is not a guard.

    Every clause the guard claims gets a planted violation, in the shape the real defect took.
    The planted sources are evaluated against an EMPTY namespace, exactly as an unknown module
    would be — so a resolver that only works by accident on the real module fails here.
    """
    assert gate_violations(_PLANTED_TABLE), "guard missed a FOURTH table read via an attribute (the V3 shape)"
    assert gate_violations(_PLANTED_TASK_VALUE), "guard missed EVIDENCE smuggled through a Databricks task value"
    assert gate_violations(_PLANTED_IMPORT), "guard missed an import of in-memory drain state (DrainSummary)"
    assert gate_violations(_PLANTED_SQL), "guard missed a raw .sql(...) read, which bypasses the allowlist"


def test_planted_violations_name_the_right_defect() -> None:
    """A guard that fires for the WRONG reason is a coincidence, not a guard."""
    assert "fct_action_context" in " ".join(gate_violations(_PLANTED_TABLE))
    assert "failed_units" in " ".join(gate_violations(_PLANTED_TASK_VALUE))
    assert _FORBIDDEN_IMPORT in " ".join(gate_violations(_PLANTED_IMPORT))
    assert "raw SQL" in " ".join(gate_violations(_PLANTED_SQL))


def test_an_allowlisted_read_in_house_style_is_NOT_flagged() -> None:  # noqa: N802
    """The negative control: the SAME attribute-assembled shape as (a), but on an allowlisted table,
    must pass. Without this, a guard that simply rejects every attribute read would look correct.
    """
    ok = (
        "class G:\n"
        "    def __init__(self, spark, catalog):\n"
        "        self._spark = spark\n"
        "        self._t = f'{catalog}.observability.{_QT}'\n"
        "    def run(self):\n"
        "        return self._spark.table(self._t)\n"
    )
    # `_QT` resolves through the namespace exactly as `_QUEUE_TABLE` does in the real gate.
    assert gate_violations(ok, {"_QT": _QUEUE_TABLE}) == []
