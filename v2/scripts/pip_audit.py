"""Run pip-audit with v2's justified ignores (spec §23.4: no unexplained ignores).

Run from v2/: `uv run python scripts/pip_audit.py`. It exits 0 only when every reported advisory has an entry in
`pip-audit-ignores.toml` and every entry still matches a reported advisory. A stale entry fails too, so an ignore is
retired as soon as its fix can install.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

V2_ROOT: Final = Path(__file__).resolve().parents[1]
IGNORES_FILE: Final = V2_ROOT / "pip-audit-ignores.toml"
REQUIRED_FIELDS: Final[tuple[str, ...]] = ("id", "package", "fix_in", "blocked_by", "justification", "review_trigger")


@dataclass(frozen=True)
class Ignore:
    id: str
    package: str
    fix_in: str
    blocked_by: str
    justification: str
    review_trigger: str


@dataclass(frozen=True)
class Finding:
    package: str
    version: str
    id: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class Evaluation:
    unexplained: list[Finding]
    stale: list[Ignore]


def load_ignores(path: Path) -> list[Ignore]:
    """Read and validate the ignore file: every entry has all six non-empty fields, and ids are unique."""
    data: dict[str, Any] = tomllib.loads(path.read_text(encoding="utf-8"))
    entries: list[dict[str, Any]] = data.get("ignore", [])
    ignores: list[Ignore] = []
    seen: set[str] = set()
    for entry in entries:
        ident = str(entry.get("id", "")).strip() or "<no id>"
        missing = [field for field in REQUIRED_FIELDS if not str(entry.get(field, "")).strip()]
        if missing:
            raise ValueError(f"ignore {ident}: missing or empty fields {missing}")
        if ident in seen:
            raise ValueError(f"duplicate ignore id {ident}")
        seen.add(ident)
        ignores.append(Ignore(**{field: str(entry[field]) for field in REQUIRED_FIELDS}))
    return ignores


def evaluate(reported: Iterable[Finding], ignores: Sequence[Ignore]) -> Evaluation:
    """Compare reported findings with the ignores: unexplained findings and stale ignores both fail."""
    ignored_ids = {entry.id for entry in ignores}
    used: set[str] = set()
    unexplained: list[Finding] = []
    for finding in reported:
        matches = ignored_ids & {finding.id, *finding.aliases}
        if matches:
            used |= matches
        else:
            unexplained.append(finding)
    stale = [entry for entry in ignores if entry.id not in used]
    return Evaluation(unexplained=unexplained, stale=stale)


def _findings(report: dict[str, Any]) -> list[Finding]:
    findings: dict[tuple[str, str, str], Finding] = {}
    for dependency in report.get("dependencies", []):
        for vuln in dependency.get("vulns", []):
            finding = Finding(
                package=str(dependency["name"]),
                version=str(dependency["version"]),
                id=str(vuln["id"]),
                aliases=tuple(str(alias) for alias in vuln.get("aliases", [])),
            )
            findings[(finding.package, finding.version, finding.id)] = finding
    return list(findings.values())


def main() -> int:
    ignores = load_ignores(IGNORES_FILE)
    result = subprocess.run(
        [sys.executable, "-m", "pip_audit", "-f", "json", "--skip-editable"],
        capture_output=True,
        text=True,
        check=False,
        cwd=V2_ROOT,
    )
    if not result.stdout.strip():
        sys.stderr.write(result.stderr)
        sys.stderr.write("pip-audit produced no report\n")
        return 2
    evaluation = evaluate(_findings(json.loads(result.stdout)), ignores)
    for finding in evaluation.unexplained:
        sys.stderr.write(f"UNEXPLAINED: {finding.package} {finding.version} {finding.id} {list(finding.aliases)}\n")
    for entry in evaluation.stale:
        sys.stderr.write(f"STALE IGNORE: {entry.id} ({entry.package}) is no longer reported; remove it\n")
    if evaluation.unexplained or evaluation.stale:
        return 1
    sys.stdout.write(f"pip-audit: clean apart from {len(ignores)} justified ignores\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
