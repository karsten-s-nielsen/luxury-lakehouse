"""Prove what each pip-audit ignore actually CLAIMS: that taking the fix is not free.

Rationale: the ADR-075 amendment (2026-08-11).

Every entry in ``.pip-audit-ignores.yml`` carries ``fix_in`` (a fix exists) and ``blocked_by``
(why we cannot take it). Read literally, ``blocked_by`` names a cap held by another package —
but that is the *mechanism*, and mechanisms and claims diverge (see below; measurement forced
this distinction). What this checks is the claim: add the floor, resolve, and require that the
resolve either fails or succeeds only by degrading the rest of the graph. When it starts
applying cleanly, the blocker has moved and the ignore is stale.

This automates the evidence the entries already carry. ``CVE-2026-28684``'s justification
reads *"VERIFIED 2026-08-08 by attempting the floor: `uv add "python-dotenv>=1.2.2"` fails as
unsatisfiable"* — a one-off hand-run whose result silently ages. The gate re-runs it.

WHY NOT READ THE CAP FROM METADATA
----------------------------------
Two earlier designs were killed by measurement, both one level below the last:

* ``uv.lock`` records ``taipy-common -> dependencies = [{ name = "deepdiff" }]`` — the **name
  only**. The version constraint is not in the lock at all.
* ``importlib.metadata.requires()`` is a real API but reads **installed** distributions.
  Measured 2026-08-10, four of the five cap holders (``taipy``, ``taipy-common``, ``mlflow``,
  ``databricks-connect``) are not installed in the dev venv — ``uv.lock`` marks taipy
  ``extra == 'taipy-app'``. It would have raised ``PackageNotFoundError`` before reaching an
  assertion, while the *capped* packages ARE installed — so a gate written against the wrong
  side of the relation reads a real version, returns a real answer, and means nothing.

THE CLAIM IS "NOT FREE", NOT "UNSATISFIABLE"
--------------------------------------------
The first version of this gate asserted *unsatisfiability*, and the first run disproved that
framing on 2 of 6 entries. Measured 2026-08-11:

* ``setuptools>=83.0.0`` resolves — by backtracking torch 2.11.0 -> 2.10.0, which does not
  depend on setuptools at all, so **setuptools leaves the lock** and the constraint is
  satisfied vacuously. A pre-flight presence check cannot see this; the package is present
  when the probe starts and absent when it ends.
* ``cryptography>=50.0.0`` resolves — by backtracking mlflow 3.15.1 -> 3.2.0 (the last release
  with no cryptography constraint), taking gunicorn, cachetools and packaging down with it,
  dropping five packages, and downgrading **flask-cors 6.0.2 -> 5.0.1, which re-opens three
  other open Dependabot alerts.**

Both entries' own justifications said so in prose ("forces torch down to 2.10.0"; "backtracks
to mlflow 3.2.0 ... dragging cachetools and gunicorn down with it"). A gate that read those as
moved blockers would have filed two false alarms on its first run and taught everyone to
ignore it.

So the claim asserted here is the decision-relevant one — **taking this fix is not free** —
which every entry makes uniformly, rather than a mechanism only some of them use.

FOUR OUTCOMES
-------------
``BLOCKED`` (unsatisfiable) and ``COLLATERAL`` (resolves only by changing other packages) both
mean the ignore is still justified. ``MOVED`` means the floor applies cleanly: take it.
``UNKNOWN`` is anything else — a resolve can fail because the network was down, and folding
that into ``BLOCKED`` would make this gate quietly certify a claim it never tested, so it is
just as loud as ``MOVED``. (ADR-068's rule, in the other direction: unverifiable is not the
same as verified-good.)

THE PROBE MECHANISM IS THE FIX MECHANISM
----------------------------------------
The floor goes into ``[tool.uv] constraint-dependencies`` — which ``pyproject.toml`` already
documents as the canonical home for "vulnerable transitive package, fix version exists". So a
probe that resolves is not merely a signal that something changed: it is a rehearsal of the
exact edit the fix requires.

Usage::

    uv run python scripts/check_cve_blockers.py             # all entries carrying fix_in
    uv run python scripts/check_cve_blockers.py --only CVE-2026-28684
    uv run python scripts/check_cve_blockers.py --timeout 1200
"""

from __future__ import annotations

import argparse
import dataclasses
import re
import subprocess
import sys
from enum import Enum
from pathlib import Path

from scripts.pip_audit_ignores import IgnoreListError, load_ignores

_REPO = Path(__file__).resolve().parents[1]
_PYPROJECT = _REPO / "pyproject.toml"
_LOCK = _REPO / "uv.lock"

#: Resolution can fork across the declared extra conflicts, so it is not fast.
DEFAULT_TIMEOUT_S = 900


class Outcome(str, Enum):
    """What a floor attempt proved. See FOUR OUTCOMES above for what each one means.

    An enum, not four bare strings, because the comparisons here fail in an asymmetric way. A
    silently-False ``Result.ok`` reports a justified ignore as failing — noisy but safe. A
    silently-False ``== MOVED`` stops the gate telling you a blocker has moved, which is the
    entire purpose of the tool. With bare strings a typo'd comparison is quietly False; here it
    is a type error.

    ``str`` mixin because Python 3.10 has no ``StrEnum`` and the summary line formats these into
    a fixed-width column; ``__str__`` is overridden so ``str(x)`` and ``f"{x}"`` agree — without
    it, 3.10 returns ``Outcome.MOVED`` from the first and ``MOVED`` from the second. The log
    output is byte-identical to the pre-enum form, asserted by
    ``test_outcome_log_column_is_unchanged``.

    Deliberately a SEPARATE type from ``audit_resolutions.Outcome``, which is CLEAN / FINDINGS /
    UNKNOWN. Same idiom, different domains: "did this resolution audit clean" and "did this floor
    resolve" are distinct questions, and the two gates fail independently.
    """

    BLOCKED = "BLOCKED"
    COLLATERAL = "COLLATERAL"
    MOVED = "MOVED"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return self.value


#: Module-level aliases. The enum is the type; these keep the ~20 existing reference sites and
#: the test suite reading as they did, rather than churning every one of them in a commit whose
#: point is that behaviour does NOT change.
BLOCKED = Outcome.BLOCKED
COLLATERAL = Outcome.COLLATERAL
MOVED = Outcome.MOVED
UNKNOWN = Outcome.UNKNOWN

#: The two outcomes that mean "the ignore is still justified". Kept as a set rather than an
#: ``in (...)`` inline so the pass condition has exactly one definition.
_PASSING = frozenset({Outcome.BLOCKED, Outcome.COLLATERAL})

#: ``fix_in`` sentinel for "no patched release exists" — the one case where an ignore cannot be
#: retired by taking a version. All six schema fields stay REQUIRED and non-blank (a blank
#: ``fix_in`` is indistinguishable from a forgotten one); this says the absence is known.
#: Same spelling the Dependabot API emits for a missing ``first_patched_version``.
NO_FIX = "NONE"


def is_checkable(entry: dict[str, str]) -> bool:
    """Can this entry's claim be tested by attempting a floor?

    ``NO_FIX`` entries cannot: there is no version to floor to. They are excluded here and
    pinned as an explicit set in the test suite, so declaring one is a visible, reviewed
    change rather than a quiet way to opt out of the gate.
    """
    fix = str(entry.get("fix_in", "")).strip()
    return bool(fix) and fix != NO_FIX


#: uv's wording for a genuine unsatisfiable resolution. Any OTHER non-zero exit is UNKNOWN.
#: Matching on this rather than on "did it fail" is what stops an offline runner from
#: reporting every blocker as intact.
_NO_SOLUTION = "No solution found when resolving"

_CONSTRAINTS_ANCHOR = "constraint-dependencies = ["

#: Strip terminal colour before storing uv's explanation — the scheduled job's log is read as
#: text and pasted into justifications.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

#: How much of uv's "no solution" chain to keep. The first lines name the package holding the
#: cap, which is the part that belongs in ``blocked_by``; the tail restates the project's own
#: requirements and is identical for every entry.
_EXPLANATION_LINES = 8


def resolver_explanation(output: str) -> tuple[str, ...]:
    """The head of uv's unsatisfiability chain — the part that names the cap holder.

    Worth capturing because a hand-written ``blocked_by`` rots silently: audited 2026-08-11,
    three of six entries named the wrong package, and one named two packages
    (``databricks-connect``/``pyspark``) that are not in the lock at all — so its
    ``review_trigger`` watched an upstream that could never have unblocked it. uv states the
    real holder every time it refuses.
    """
    clean = _ANSI_RE.sub("", output)
    start = clean.find(_NO_SOLUTION)
    if start < 0:
        return ()
    body = [line.rstrip() for line in clean[start:].splitlines()]
    return tuple(line for line in body[:_EXPLANATION_LINES] if line.strip())


#: uv.lock writes ``name`` then ``version`` on the two lines after each ``[[package]]`` header.
_LOCK_PACKAGE_RE = re.compile(r'^\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', re.MULTILINE)


class ProbeError(RuntimeError):
    """The probe could not be set up — refuse rather than run a check that proves nothing."""


@dataclasses.dataclass(frozen=True)
class Result:
    """One entry's verdict, plus the graph changes that produced it."""

    advisory: str
    package: str
    floor: str
    outcome: Outcome
    detail: str
    collateral: tuple[str, ...] = ()
    explanation: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """An intact blocker is a pass, whether it blocks outright or only at a cost.

        UNKNOWN is a failure, not a shrug.
        """
        return self.outcome in _PASSING


def normalize(name: str) -> str:
    """PEP 503 normalisation, so ``Twisted`` and ``twisted`` are one package."""
    return re.sub(r"[-_.]+", "-", name).lower()


def package_name(entry: dict[str, str]) -> str:
    """First token of the free-text ``package`` field.

    Entries read like ``python-dotenv (1.2.1 in the base resolution; ...)`` or
    ``pyarrow 19.0.1`` — prose written for a human, with the name always leading.
    """
    raw = str(entry["package"]).strip()
    if not raw:
        raise ProbeError("entry has a blank package field")
    return raw.split()[0]


def lock_versions(lock_text: str) -> dict[str, tuple[str, ...]]:
    """Normalised name -> every version of it pinned in the lock.

    A TUPLE, not a single version: ``pyproject.toml`` declares extra ``conflicts``
    (taipy-app / dbt / sdk), so uv writes a FORKED lock in which one package can appear more
    than once. Measured on the 2026-08-11 lock: 9 of 298 names have two entries, including
    ``python-dotenv`` (1.0.1 and 1.2.1) and ``pyarrow`` (18.1.0 and 19.0.1) — two of the six
    ignore entries. A ``{name: version}`` comprehension silently keeps whichever block comes
    last, so a change confined to the other fork is invisible.
    """
    found: dict[str, list[str]] = {}
    for match in _LOCK_PACKAGE_RE.finditer(lock_text):
        found.setdefault(normalize(match.group(1)), []).append(match.group(2))
    return {name: tuple(sorted(versions)) for name, versions in found.items()}


def locked_packages(lock_text: str) -> set[str]:
    """Normalised names of every package in the lock."""
    return set(lock_versions(lock_text))


def graph_changes(
    before: dict[str, tuple[str, ...]], after: dict[str, tuple[str, ...]], target: str
) -> tuple[list[str], bool]:
    """What the floor did to the resolution, besides floor the target.

    Returns the collateral lines (every OTHER package added, removed or re-versioned) and
    whether the target itself left the graph.

    ``uv lock`` is deterministic given identical inputs, so every difference here is caused by
    the constraint — there is no incidental churn to filter out.

    Direction is deliberately not labelled. Calling 2.11.0 -> 2.10.0 a "downgrade" needs
    PEP 440 comparison, and a mislabelled direction in a security report is worse than an
    unlabelled one. Both versions are printed; the reader can see which way it went.
    """
    key = normalize(target)
    names = sorted((before.keys() | after.keys()) - {key})
    lines = [
        f"{name}: {_render(before.get(name), 'ABSENT')} -> {_render(after.get(name), 'REMOVED')}"
        for name in names
        if before.get(name) != after.get(name)
    ]
    return lines, key in before and key not in after


def _render(versions: tuple[str, ...] | None, missing: str) -> str:
    """Forked packages print every pinned version, so a fork-local change is legible.

    ``missing`` differs by side on purpose: a package absent BEFORE was added, one absent
    AFTER was dropped. Printing "REMOVED" on both sides renders an addition as
    ``REMOVED -> 3.0``, which reads as the opposite of what happened.
    """
    if not versions:
        return missing
    return ", ".join(versions)


def inject_constraint(pyproject_text: str, requirement: str) -> str:
    """Add ``requirement`` to ``[tool.uv] constraint-dependencies``.

    A constraint floors a version WITHOUT adding a direct dependency — which is what makes
    this a faithful probe for transitive packages. It also means a constraint on a package
    that is not in the graph is inert, so callers must confirm presence in the lock first
    (an inert constraint resolves cleanly and would read as ``MOVED``).
    """
    if _CONSTRAINTS_ANCHOR not in pyproject_text:
        raise ProbeError(
            f"{_PYPROJECT.name} has no `{_CONSTRAINTS_ANCHOR}` — the probe writes there because "
            "that is where the repo documents transitive CVE floors belong."
        )
    return pyproject_text.replace(
        _CONSTRAINTS_ANCHOR,
        f'{_CONSTRAINTS_ANCHOR}\n    "{requirement}",  # check_cve_blockers probe (transient)',
        1,
    )


def classify(
    returncode: int,
    output: str,
    *,
    collateral: list[str] | None = None,
    target_vanished: bool = False,
) -> tuple[Outcome, str]:
    """Map a ``uv lock`` result and its graph diff onto an outcome and a detail line.

    A clean exit alone does not mean the blocker moved — see the module docstring. ``MOVED``
    requires that the target was floored AND nothing else in the resolution moved to allow it.
    """
    if returncode != 0:
        if _NO_SOLUTION in output:
            return BLOCKED, "uv reports no solution — the cap still binds"
        return UNKNOWN, f"uv lock failed for a reason that is not unsatisfiability (exit {returncode})"

    changes = collateral or []
    if target_vanished:
        return COLLATERAL, (
            f"resolved, but the target LEFT the lock — the constraint was satisfied vacuously "
            f"by dropping it, alongside {len(changes)} other package change(s)"
        )
    if changes:
        return COLLATERAL, f"resolved, but only by changing {len(changes)} other package(s)"
    return MOVED, "uv lock succeeded cleanly — the floor applies with no other package moving"


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 — fixed argv, no shell
        ["git", *args],  # noqa: S607 — git resolved from PATH, as everywhere else in scripts/
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
    )


def assert_probe_files_clean() -> None:
    """Refuse to run against uncommitted changes to the files the probe restores.

    The restore is ``git checkout -- pyproject.toml uv.lock``, which cannot leave a dirty
    tree after an aborted probe — but which would DESTROY pre-existing edits to those two
    files. The guard is what makes 'the restore is safe' true rather than merely usual.
    """
    status = _git("status", "--porcelain", "--", str(_PYPROJECT.name), str(_LOCK.name))
    if status.stdout.strip():
        raise ProbeError(
            "pyproject.toml and/or uv.lock have uncommitted changes:\n"
            f"{status.stdout.strip()}\n"
            "This probe restores them with `git checkout --`, which would discard that work. "
            "Commit or stash first."
        )


def restore_probe_files() -> None:
    """Undo the probe. Runs in a ``finally`` so an abort mid-resolve still cleans up."""
    result = _git("checkout", "--", _PYPROJECT.name, _LOCK.name)
    if result.returncode != 0:
        raise ProbeError(f"failed to restore {_PYPROJECT.name}/{_LOCK.name}: {result.stderr.strip()}")


def check_entry(entry: dict[str, str], *, timeout_s: int = DEFAULT_TIMEOUT_S) -> Result:
    """Attempt one floor and report whether it still fails to resolve.

    One package per resolve, deliberately: a batch failure does not say which floor caused it.
    """
    advisory = str(entry["id"])
    package = package_name(entry)
    floor = f"{package}>={entry['fix_in']}"

    before = lock_versions(_LOCK.read_text(encoding="utf-8"))
    if normalize(package) not in before:
        return Result(
            advisory,
            package,
            floor,
            UNKNOWN,
            f"{package} is not in uv.lock, so a constraint on it is inert and would resolve "
            "cleanly regardless of any cap. Fix the entry's package field or drop the entry.",
        )

    original = _PYPROJECT.read_text(encoding="utf-8")
    try:
        _PYPROJECT.write_text(inject_constraint(original, floor), encoding="utf-8")
        try:
            proc = subprocess.run(
                ["uv", "lock"],  # noqa: S607 — uv resolved from PATH, as in every CI workflow
                cwd=_REPO,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_s,
            )
        except subprocess.TimeoutExpired:
            return Result(advisory, package, floor, UNKNOWN, f"uv lock exceeded {timeout_s}s — no verdict")

        changes: list[str] = []
        vanished = False
        if proc.returncode == 0:
            # Read the lock BEFORE the finally-block restores it.
            changes, vanished = graph_changes(before, lock_versions(_LOCK.read_text(encoding="utf-8")), package)
        output = proc.stdout + proc.stderr
        outcome, detail = classify(proc.returncode, output, collateral=changes, target_vanished=vanished)
        return Result(advisory, package, floor, outcome, detail, tuple(changes), resolver_explanation(output))
    finally:
        restore_probe_files()


def format_failure(entry: dict[str, str], result: Result) -> str:
    """Quote the entry's own recorded expectations back at the reader.

    A bare "this resolved now" leaves the next person re-deriving why the ignore existed;
    ``blocked_by`` and ``review_trigger`` are exactly that context, already written down.
    """
    headline = (
        f"{result.advisory}: {result.floor} NOW RESOLVES — the blocker has moved."
        if result.outcome == MOVED
        else f"{result.advisory}: {result.floor} gave NO VERDICT."
    )
    return (
        f"{headline}\n"
        f"    detail:         {result.detail}\n"
        f"    blocked_by:     {entry['blocked_by']}\n"
        f"    review_trigger: {entry['review_trigger']}\n"
        + ("    ACTION: apply the floor and drop the ignore.\n" if result.outcome == MOVED else "")
    )


def main(argv: list[str] | None = None) -> int:
    """Check every ignore that carries a ``fix_in``. Non-zero if any is not BLOCKED."""
    parser = argparse.ArgumentParser(description="Verify pip-audit ignore blockers still block")
    parser.add_argument("--only", help="check a single advisory id")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help=f"per-resolve timeout in seconds (default {DEFAULT_TIMEOUT_S})",
    )
    args = parser.parse_args(argv)

    try:
        entries = [e for e in load_ignores() if is_checkable(e)]
    except IgnoreListError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.only:
        entries = [e for e in entries if str(e["id"]) == args.only]
        if not entries:
            print(f"ERROR: no checkable ignore entry with id {args.only!r}", file=sys.stderr)
            return 1

    if not entries:
        print(f"ERROR: every entry is {NO_FIX} — this gate would cover nothing", file=sys.stderr)
        return 1

    try:
        assert_probe_files_clean()
    except ProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        f"Attempting {len(entries)} floor(s); each should be {BLOCKED} or {COLLATERAL}.\n",
        file=sys.stderr,
    )
    results: list[tuple[dict[str, str], Result]] = []
    for entry in entries:
        try:
            result = check_entry(entry, timeout_s=args.timeout)
        except ProbeError as exc:
            result = Result(str(entry["id"]), package_name(entry), "?", UNKNOWN, str(exc))
        results.append((entry, result))
        print(f"  {result.outcome:10s} {result.advisory:20s} {result.floor:28s} {result.detail}", file=sys.stderr)
        # Both of these are the EVIDENCE for the entry's blocked_by, printed every run so the
        # justification is re-derived rather than dated to whenever someone last looked. The
        # 2026-08-11 audit found three of six blocked_by records naming the wrong package;
        # uv's own explanation names the right one, and the collateral list is the whole
        # reason a COLLATERAL entry stays ignored.
        for line in result.explanation:
            print(f"      > {line}", file=sys.stderr)
        for line in result.collateral:
            print(f"      | {line}", file=sys.stderr)

    failures = [(e, r) for e, r in results if not r.ok]
    print(file=sys.stderr)
    if not failures:
        print(f"OK: all {len(results)} blocker(s) still block. Ignores remain justified.", file=sys.stderr)
        return 0

    print(f"{len(failures)} of {len(results)} entr(ies) need action:\n", file=sys.stderr)
    for entry, result in failures:
        print(format_failure(entry, result), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
