"""Emit pip-audit ``--ignore-vuln`` flags from the versioned ignore list (SEC5).

Single source of truth: ``.pip-audit-ignores.yml``. Both pip-audit invocations in
``python-ci.yml`` — "Audit dependencies" and "Generate SBOM (CycloneDX)" — call this, so
the two lists cannot drift. They previously carried 16 inline flags EACH, kept in sync by
nothing; adding an ignore to one and not the other turned CI red a second time on
2026-08-07.

Usage::

    uv run pip-audit $(uv run python scripts/pip_audit_ignores.py)
    uv run python scripts/pip_audit_ignores.py --check    # validate the file, emit nothing

``--check`` is the non-mutating validation used by the test suite: schema completeness and
duplicate detection, so a malformed entry fails fast rather than silently dropping an
ignore and turning CI red for an unrelated-looking reason.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

_IGNORE_FILE = Path(__file__).resolve().parents[1] / ".pip-audit-ignores.yml"

REQUIRED_FIELDS = ("id", "package", "fix_in", "blocked_by", "justification", "review_trigger")


class IgnoreListError(ValueError):
    """The ignore list is malformed — fail loudly rather than emit a partial flag set."""


def load_ignores(path: Path | None = None) -> list[dict[str, str]]:
    """Parse and validate the ignore list.

    Raises:
        IgnoreListError: on a missing/empty file, a missing or blank required field, or a
            duplicate advisory id. Every one of these would otherwise silently change which
            advisories CI suppresses.
    """
    target = path or _IGNORE_FILE
    if not target.exists():
        raise IgnoreListError(f"{target} not found — CI cannot resolve its pip-audit ignores")

    parsed = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    entries = parsed.get("ignores")
    if not entries:
        raise IgnoreListError(f"{target} has no 'ignores' entries")

    seen: set[str] = set()
    for entry in entries:
        missing = [f for f in REQUIRED_FIELDS if not str(entry.get(f, "")).strip()]
        if missing:
            raise IgnoreListError(f"entry {entry.get('id', '<no id>')!r} is missing/blank: {missing}")
        advisory = str(entry["id"])
        if advisory in seen:
            raise IgnoreListError(f"duplicate advisory id {advisory!r}")
        seen.add(advisory)
    return entries


def flags(entries: list[dict[str, str]]) -> list[str]:
    """Render entries as pip-audit CLI flags."""
    out: list[str] = []
    for entry in entries:
        out += ["--ignore-vuln", str(entry["id"])]
    return out


def main() -> None:
    """Print the flag string, or validate with --check."""
    parser = argparse.ArgumentParser(description="Emit pip-audit --ignore-vuln flags")
    parser.add_argument("--check", action="store_true", help="validate the ignore list; emit nothing")
    args = parser.parse_args()

    try:
        entries = load_ignores()
    except IgnoreListError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    if args.check:
        print(f"OK: {len(entries)} ignore entries, all fields present, no duplicates", file=sys.stderr)
        return

    print(" ".join(flags(entries)))


if __name__ == "__main__":
    main()
