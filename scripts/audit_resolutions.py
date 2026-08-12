"""Audit every RESOLUTION in ``uv.lock``, not just the environment CI happens to install.

Rationale: the ADR-075 amendment (2026-08-11), "Audit RESOLUTIONS, not the environment that
happens to be installed".

WHY THIS EXISTS
---------------
``uv run pip-audit`` audits the *installed* environment, which is the base resolution plus the
dev group. Production is not that environment. The Taipy Space deploys with the ``taipy-app``
extra, and measured 2026-08-11 that resolution carries **17 findings across 11 packages**
against the base environment's 7 across 5 — eight advisories that CI had never once looked at,
including a high-severity Taipy issue with no patched release.

`pyproject.toml` declares ``taipy-app`` / ``dbt`` / ``sdk`` as CONFLICTING extras, so no single
environment can contain them all; uv writes a forked lock instead. That is precisely why an
installed-environment audit cannot cover the project — there is no environment to install. Each
fork has to be exported and audited on its own.

Measured coverage on 2026-08-11 — ``taipy-app`` is a superset today, but the others are audited
anyway so that a future dbt- or sdk-only advisory is not invisible by construction:

===========  ==================  ==========
resolution   findings/packages   new ids
===========  ==================  ==========
base                7 / 5        (baseline)
taipy-app          17 / 11       8
dbt                 8 / 6        0
sdk                 6 / 5        0
===========  ==================  ==========

THREE OUTCOMES, DECIDED FROM THE REPORT
---------------------------------------
Every target resolves to exactly one ``Outcome``, and the distinction between the last two is the
reason this module was rewritten:

============  ==============================================================================
``CLEAN``     pip-audit produced a report and no dependency carries an unignored advisory.
``FINDINGS``  pip-audit produced a report and something in it is vulnerable. Act on the CVE.
``UNKNOWN``   pip-audit did not reach a verdict. **This is an infrastructure failure, not a
              set of CVE regressions.** Fix the runner; read nothing into it until you have.
============  ==============================================================================

On 2026-08-11 the job's first run on a GitHub runner printed ``FAIL: unignored findings in 4
resolution(s): base, taipy-app, dbt, sdk``. There were no findings. ``audit()`` had built the
editable install, hit the hatchling force-include of the gitignored ``dbt_project/dbt_packages``,
and ``main()`` mapped the non-zero exit to "vulnerabilities found" — four fabricated CVE
regressions from one missing directory. This is the ``BLOCKED``/``UNKNOWN`` rule
``check_cve_blockers.py`` already applies (*"unverifiable is not the same as verified-good"*),
which had not been carried across to its sibling.

Two properties follow, and both are enforced by tests rather than by convention:

* **The verdict comes from the JSON report, never from the exit code and never from prose.**
  Matching an upstream tool's wording would make a security gate depend on something that is not
  an API. The one stderr signal that must count — a dependency that could not be collected —
  arrives structurally instead, as a non-zero exit under ``--strict``.
  (``test_stderr_never_changes_the_verdict``)
* **The evidence travels with the verdict.** ``AuditResult.diagnostics`` carries what the tool
  actually said, and it is printed on any non-CLEAN outcome. Dropping it at the subprocess
  boundary is irreversible; declining to print it is a policy the caller can change. An UNKNOWN
  reading "fix the runner" with the traceback discarded is an instruction with no evidence.

Where output is elided — the diagnostic bound, the named-package list — the amount dropped is
always stated. A silent truncation is a quieter version of the same bug.

THE LOCAL-VERSION PROXY (an approximation, deliberately visible)
---------------------------------------------------------------
``pip-audit -r`` dry-run-installs the requirements into a throwaway env, so a PEP 440 local
version that exists on no index aborts the entire audit:

    ERROR: Could not find a version that satisfies the requirement torch==2.11.0+cu128

Our torch comes from the ``pytorch-cu128`` index. Two options were measured:

* ``--extra-index-url https://download.pytorch.org/whl/cu128`` — pip then RESOLVES it, but
  pip-audit still reports ``Dependency not found on PyPI and could not be audited``, so torch
  stays unaudited (16 findings / 10 packages). It fixes the abort, not the blind spot.
* Strip the local segment, auditing ``torch==2.11.0`` as a proxy — 17 / 11, and torch's
  advisory (PYSEC-2025-194) is found.

We strip. ``2.11.0+cu128`` is the same upstream source tree as ``2.11.0`` at the same version,
so advisories apply identically; the local segment records the CUDA build, not different code.
This IS an approximation, so every strip is logged on every run rather than done quietly — a
security gate that silently substitutes its input is the failure mode this repo keeps paying
for. Without it torch is skipped entirely, which is strictly worse and was the status quo.

WHAT IS AUDITED IS PLATFORM-DEPENDENT — A LOCAL PASS IS NOT A CI PASS
---------------------------------------------------------------------
``pip-audit -r`` evaluates environment markers, so the set it audits depends on the machine
running it. The export is platform-independent; the audit is not.

Measured 2026-08-11, markers evaluated per platform:

===========  ==========  ===========  ============  ==========================
resolution   exported    linux (CI)   win (local)   audited ONLY on linux
===========  ==========  ===========  ============  ==========================
base                55           54            55   --
taipy-app          141          140           141   --
dbt                116          115           114   jeepney, secretstorage
sdk                 62           61            62   --
===========  ==========  ===========  ============  ==========================

`jeepney` and `secretstorage` are `sys_platform == 'linux'`: the weekly job audits them and no
local run on Windows ever will. `colorama` and `pywin32-ctypes` are the mirror image — audited
locally, never by CI, and harmless because neither can execute in the Space's linux container.

**The weekly linux job is the authority; a local run is a convenience approximation.** A local
"all targets CLEAN" is evidence, not proof. This is recorded rather than papered over for the same
reason the local-version proxy above is logged on every run: a gate whose coverage differs from
what the reader assumes is the failure mode this repo keeps paying for.

Usage::

    uv run python scripts/audit_resolutions.py              # all resolutions
    uv run python scripts/audit_resolutions.py --only taipy-app
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from scripts.pip_audit_ignores import flags, load_ignores

_REPO = Path(__file__).resolve().parents[1]

#: ``None`` is the base resolution (no extra). Order is deliberate: the production surface
#: first, so its output is at the top of a failing log.
RESOLUTIONS: tuple[str | None, ...] = (None, "taipy-app", "dbt", "sdk")

#: A pinned requirement carrying a PEP 440 local version (``name==1.2.3+local``). Anchored per
#: line; the trailing group stops at whitespace or a marker so ``; python_version < "3.11"``
#: survives untouched.
_LOCAL_VERSION_RE = re.compile(r"^(?P<pin>[A-Za-z0-9._-]+==[^+\s;]+)\+(?P<local>[A-Za-z0-9.]+)", re.MULTILINE)

_EXPORT_TIMEOUT_S = 300
_AUDIT_TIMEOUT_S = 1800

#: Pinned deliberately. pip-audit's JSON shape and --strict semantics are the gate's contract;
#: a silent upgrade could change either. Bump this in a reviewed commit, never implicitly.
_PIP_AUDIT_VERSION = "2.10.1"

#: Both ends of a captured stream are kept when it is too long to print whole. Two ends because
#: the two failures this gate sees put the answer in different places: a uv resolver error leads
#: with its summary, a Python traceback ends with the exception type and message.
_DIAGNOSTIC_HEAD_LINES = 20
_DIAGNOSTIC_TAIL_LINES = 20

#: How many vulnerable packages the one-line detail names before saying "and N more". Same rule
#: as the diagnostic bound: truncate if you must, but never silently.
_MAX_NAMED_PACKAGES = 5


class Outcome(str, Enum):
    """What the audit concluded. An enum, not three bare strings, so a typo'd comparison is a
    type error rather than a silently-False one.

    ``str`` mixin because Python 3.10 has no ``StrEnum`` and the summary formats these into a
    fixed-width column. ``__str__`` is overridden so ``str(x)`` and ``f"{x}"`` agree — without it
    3.10 gives ``Outcome.CLEAN`` for the first and ``CLEAN`` for the second.
    """

    CLEAN = "CLEAN"
    FINDINGS = "FINDINGS"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class AuditResult:
    """One target's verdict, together with the evidence behind it.

    ``diagnostics`` is what the tool actually said. It is carried here rather than dropped at the
    subprocess boundary because discarding it there is irreversible, whereas declining to print
    it is a policy the caller can change. An UNKNOWN that says "fix the runner" while withholding
    the traceback is an instruction with no evidence.

    It is NEVER an input to ``outcome`` — see ``classify_audit``.
    """

    outcome: Outcome
    detail: str
    diagnostics: str = ""


def _collect_diagnostics(stderr: str, *, unparsed_stdout: str = "") -> str:
    """Gather the tool output a human needs when the verdict is not CLEAN.

    stderr always. stdout ONLY when it failed to parse as the JSON report: at that point it is
    not a report, it is evidence, and keeping just one of the two streams would discard half of
    what the tool said. On a parseable run stdout is the report itself and adding it here would
    bury the diagnostics in it.
    """
    parts: list[str] = []
    if unparsed_stdout.strip():
        parts.append(f"--- stdout (not a JSON report) ---\n{unparsed_stdout.strip()}")
    if stderr.strip():
        parts.append(f"--- stderr ---\n{stderr.strip()}")
    return "\n".join(parts)


def bound_diagnostics(
    text: str,
    *,
    head: int = _DIAGNOSTIC_HEAD_LINES,
    tail: int = _DIAGNOSTIC_TAIL_LINES,
) -> str:
    """Keep both ends of a long diagnostic blob, and SAY how much was dropped.

    A size bound is itself something that can hide the answer, so the elision is always
    announced. Silent truncation would be a quieter version of the bug that retaining
    diagnostics fixes.
    """
    lines = text.strip().splitlines()
    if len(lines) <= head + tail:
        return "\n".join(lines)
    omitted = len(lines) - head - tail
    return "\n".join([*lines[:head], f"... {omitted} line(s) omitted ...", *lines[-tail:]])


def classify_audit(returncode: int, stdout: str, stderr: str = "") -> AuditResult:
    """Decide from the REPORT, never from the exit code and never from prose.

    pip-audit -f json emits ``{"dependencies": [{"name", "version", "vulns"}], "fixes": []}``, and
    it REMOVES ignored advisories from that report (verified 2026-08-11: ``--ignore-vuln`` on a
    ``flask==3.1.1`` requirements file yields exit 0 and ``"vulns": []``). So an empty vulns list
    across every dependency is the clean signal, and no cross-referencing against the ignore file
    is needed. If the structure is absent the tool did not complete, whatever it exited with.

    ``stderr`` is accepted so the returned result carries the evidence, and for NO other reason.
    Matching it would make a security gate depend on an upstream tool's wording, which is not an
    API — the one stderr signal that must affect the verdict, "dependency could not be collected",
    already reaches us structurally as a non-zero exit under ``--strict``. Enforced by
    ``test_stderr_never_changes_the_verdict``.
    """
    try:
        report = json.loads(stdout)
        deps = report["dependencies"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return AuditResult(
            Outcome.UNKNOWN,
            f"pip-audit did not produce a JSON report (exit {returncode})",
            _collect_diagnostics(stderr, unparsed_stdout=stdout),
        )
    diagnostics = _collect_diagnostics(stderr)
    if not isinstance(deps, list):
        return AuditResult(
            Outcome.UNKNOWN,
            f"pip-audit's JSON has no dependencies array (exit {returncode})",
            diagnostics,
        )
    vulnerable = [d for d in deps if d.get("vulns")]
    if vulnerable:
        shown = vulnerable[:_MAX_NAMED_PACKAGES]
        names = ", ".join(f"{d['name']} {d['version']}" for d in shown)
        if len(vulnerable) > len(shown):
            names += f", and {len(vulnerable) - len(shown)} more"
        detail = f"{len(vulnerable)} package(s) with unignored advisories: {names}"
        return AuditResult(Outcome.FINDINGS, detail, diagnostics)
    if returncode != 0:
        # A parseable report with no findings AND a non-zero exit is self-contradictory — the
        # shape a partial --strict collection failure takes. Reading it as CLEAN would certify
        # a set pip-audit is itself telling us it could not fully assess.
        return AuditResult(
            Outcome.UNKNOWN,
            f"pip-audit reported no findings but exited {returncode}",
            diagnostics,
        )
    return AuditResult(Outcome.CLEAN, f"{len(deps)} package(s) audited, no unignored advisories", diagnostics)


def label(extra: str | None) -> str:
    """Human name for a resolution."""
    return extra or "base"


def strip_local_versions(requirements: str) -> tuple[str, list[str]]:
    """Drop PEP 440 local segments from pinned requirements.

    Returns the rewritten text and a description of every substitution made, so the caller can
    report them. Generic rather than torch-specific: any local version aborts ``pip-audit -r``
    the same way, and hard-coding one package would let the next one fail as a mystery.
    """
    substitutions: list[str] = []

    def _replace(match: re.Match[str]) -> str:
        substitutions.append(f"{match.group('pin')}+{match.group('local')} -> {match.group('pin')}")
        return match.group("pin")

    return _LOCAL_VERSION_RE.sub(_replace, requirements), substitutions


def _export_cmd(extra: str | None) -> list[str]:
    """Build the ``uv export`` argv for one resolution.

    ``--no-default-groups`` is UNCONDITIONAL. uv export includes dependency groups by default, and
    this project's dev group is ~96 packages of test/lint/ML tooling that no deployed artifact
    contains — ``torch``, the ``nvidia-*-cu12`` CUDA stack, ``pytest``, ``ruff``, ``pyright``,
    ``scikit-learn``, ``openevolve``, and ``pip-audit`` itself. Auditing them here reported dev
    tooling as PRODUCTION exposure.

    Dev tooling is audited by ``python-ci.yml`` against the installed environment on every PR — a
    superset of every fork's dev-side, measured 2026-08-11 with markers evaluated for the runner's
    platform (216 packages, 0 uncovered across all four forks) — so re-auditing it weekly here
    would add no coverage while giving one advisory two owners.

    Split from execution so the flags are unit-testable without spawning uv.
    """
    cmd = [
        "uv",
        "export",
        "--no-hashes",
        "--no-emit-project",
        "--format",
        "requirements-txt",
        "--no-default-groups",
    ]
    if extra is not None:
        cmd += ["--extra", extra]
    return cmd


def export_resolution(extra: str | None) -> str:
    """``uv export`` one resolution as pinned requirements.

    ``--no-hashes`` because pip-audit's resolver rejects a partially-hashed file, and
    ``--no-emit-project`` because the project itself is not on any index (it would abort the
    audit exactly as an unresolvable local version does).
    """
    cmd = _export_cmd(extra)
    result = subprocess.run(  # noqa: S603 — fixed argv built from a module constant, no shell
        cmd,
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
        timeout=_EXPORT_TIMEOUT_S,
    )
    if result.returncode != 0:
        raise RuntimeError(f"uv export failed for {label(extra)}: {result.stderr.strip()}")
    return result.stdout


def audit(requirements_path: Path) -> AuditResult:
    """Run pip-audit over a requirements file and classify the result.

    ``--no-project`` so uv does NOT build the editable install: pip-audit audits a requirements
    FILE and never needed the project. Building it hits the hatchling force-include of the
    gitignored ``dbt_project/dbt_packages`` and fails on any clean checkout.

    ``--no-deps`` audits the pinned lines as given. The file is a complete locked resolution,
    so re-resolving would both be slower and audit versions we do not ship.

    ``--strict`` so a dependency that could not be collected fails instead of passing as clean.
    ``pip-audit`` is PINNED — an unpinned security tool changes behaviour under the gate silently.
    """
    cmd = [
        "uv",
        "run",
        "--no-project",
        "--with",
        f"pip-audit=={_PIP_AUDIT_VERSION}",
        "pip-audit",
        "-r",
        str(requirements_path),
        "--no-deps",
        "--strict",
        "-f",
        "json",
        *flags(load_ignores()),
    ]
    result = subprocess.run(  # noqa: S603 — argv from a constant plus the generated ignore flags
        cmd,
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
        timeout=_AUDIT_TIMEOUT_S,
    )
    return classify_audit(result.returncode, result.stdout, result.stderr)


def audit_resolution(extra: str | None, *, tmp_dir: Path) -> tuple[AuditResult, list[str]]:
    """Export, de-localise and audit one resolution."""
    exported = export_resolution(extra)
    rewritten, substitutions = strip_local_versions(exported)
    path = tmp_dir / f"requirements-{label(extra)}.txt"
    path.write_text(rewritten, encoding="utf-8")
    return audit(path), substitutions


def report_diagnostics(name: str, result: AuditResult) -> None:
    """Print the captured tool output for a verdict that is not CLEAN.

    Only when it is not CLEAN: pip-audit writes progress to stderr, and 163 clean packages of it
    is how a log stops being read.
    """
    if not result.diagnostics:
        return
    print(f"  --- {name}: captured tool output ---", file=sys.stderr)
    print(textwrap.indent(bound_diagnostics(result.diagnostics), "  "), file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    """Audit each resolution; non-zero if any reports a finding the ignore list does not cover."""
    parser = argparse.ArgumentParser(description="Audit every uv.lock resolution, not just the installed env")
    parser.add_argument("--only", help=f"audit a single resolution ({', '.join(label(r) for r in RESOLUTIONS)})")
    args = parser.parse_args(argv)

    targets = RESOLUTIONS
    if args.only:
        targets = tuple(r for r in RESOLUTIONS if label(r) == args.only)
        if not targets:
            print(f"ERROR: unknown resolution {args.only!r}", file=sys.stderr)
            return 1

    failures: list[tuple[str, AuditResult]] = []
    with tempfile.TemporaryDirectory() as tmp:
        for extra in targets:
            name = label(extra)
            print(f"\n=== auditing resolution: {name} ===", file=sys.stderr)
            result, substitutions = audit_resolution(extra, tmp_dir=Path(tmp))
            for note in substitutions:
                print(f"  local-version proxy: {note}", file=sys.stderr)
            print(f"  {result.outcome}: {result.detail}", file=sys.stderr)
            if result.outcome is not Outcome.CLEAN:
                report_diagnostics(name, result)
                failures.append((name, result))

    print(file=sys.stderr)
    if failures:
        outcomes = {result.outcome for _, result in failures}
        print(f"FAIL: {len(failures)} resolution(s) not clean:", file=sys.stderr)
        for name, result in failures:
            print(f"  {result.outcome:8s} {name}: {result.detail}", file=sys.stderr)
        if Outcome.FINDINGS in outcomes:
            print(
                "\nFINDINGS: add each advisory to .pip-audit-ignores.yml with a blocked_by "
                "re-derived by EXECUTION (scripts/check_cve_blockers.py), or take the fix.",
                file=sys.stderr,
            )
        if Outcome.UNKNOWN in outcomes:
            print(
                "\nUNKNOWN means the audit did not run — this is an infrastructure failure, "
                "NOT a set of CVE regressions. Fix the runner before reading anything into it. "
                "The captured tool output is printed above each UNKNOWN.",
                file=sys.stderr,
            )
        return 1
    print(f"OK: {len(targets)} resolution(s) clean against the shared ignore list.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
