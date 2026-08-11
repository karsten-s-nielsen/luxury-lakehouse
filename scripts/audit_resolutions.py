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

Usage::

    uv run python scripts/audit_resolutions.py              # all resolutions
    uv run python scripts/audit_resolutions.py --only taipy-app
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
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


def export_resolution(extra: str | None) -> str:
    """``uv export`` one resolution as pinned requirements.

    ``--no-hashes`` because pip-audit's resolver rejects a partially-hashed file, and
    ``--no-emit-project`` because the project itself is not on any index (it would abort the
    audit exactly as an unresolvable local version does).
    """
    cmd = ["uv", "export", "--no-hashes", "--no-emit-project", "--format", "requirements-txt"]
    if extra is not None:
        cmd += ["--extra", extra]
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


def audit(requirements_path: Path) -> tuple[int, str]:
    """Run pip-audit over a requirements file with the shared ignore list applied.

    ``--no-deps`` audits the pinned lines as given. The file is a complete locked resolution,
    so re-resolving would both be slower and audit versions we do not ship.
    """
    cmd = [
        "uv",
        "run",
        "pip-audit",
        "-r",
        str(requirements_path),
        "--no-deps",
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
    return result.returncode, result.stdout + result.stderr


def audit_resolution(extra: str | None, *, tmp_dir: Path) -> tuple[int, str, list[str]]:
    """Export, de-localise and audit one resolution."""
    exported = export_resolution(extra)
    rewritten, substitutions = strip_local_versions(exported)
    path = tmp_dir / f"requirements-{label(extra)}.txt"
    path.write_text(rewritten, encoding="utf-8")
    code, output = audit(path)
    return code, output, substitutions


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

    failures: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        for extra in targets:
            name = label(extra)
            print(f"\n=== auditing resolution: {name} ===", file=sys.stderr)
            code, output, substitutions = audit_resolution(extra, tmp_dir=Path(tmp))
            for note in substitutions:
                print(f"  local-version proxy: {note}", file=sys.stderr)
            print(output.rstrip(), file=sys.stderr)
            if code != 0:
                failures.append(name)

    print(file=sys.stderr)
    if failures:
        print(
            f"FAIL: unignored findings in {len(failures)} resolution(s): {', '.join(failures)}.\n"
            "Add each advisory to .pip-audit-ignores.yml with a blocked_by re-derived by "
            "EXECUTION (scripts/check_cve_blockers.py), or take the fix.",
            file=sys.stderr,
        )
        return 1
    print(f"OK: {len(targets)} resolution(s) clean against the shared ignore list.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
