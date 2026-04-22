"""Scan staging SQL for silent null-collapse patterns (Mode 5).

Pattern detected: ``coalesce(X, 'false')``, ``coalesce(X, false)``,
``ifnull(X, false)``, ``nvl(X, 0)``, ``coalesce(X, 0)`` on bronze columns
typed as nullable — these collapse NULL → False (or 0), destroying the
"unknown vs recorded-as-false" distinction. Mode 5 (silent sentinel
substitution) failure from the PR #173 bronze drop-safety audit (G4b).

The test walks every ``stg_*.sql`` file under
``dbt_project/models/staging/``, extracts matching patterns, and FAILs
for any pattern not listed in ``_INTENTIONAL_COALESCES`` (which requires
a non-empty reason string).

Intentionally DOES NOT flag:
  - ``coalesce(x, 'Unknown')`` / other string sentinels — those preserve
    a distinguishable marker.
  - Commented-out SQL.
  - Intermediate / mart models — cross-provider UNION conventions in
    ``int_unified_passes`` legitimately coalesce provider-specific NULLs
    to false when the source's NULL semantics are "absence = false" (e.g.
    StatsBomb only tags ``pass.cross`` when true).
"""

from __future__ import annotations

import re
from pathlib import Path

_STAGING_DIR = Path(__file__).parent.parent.parent / "dbt_project" / "models" / "staging"

# Patterns that collapse NULL → False/0/''. The capture group is the
# column expression (typically a simple identifier, but some dialects
# allow qualified or casted expressions — we match a single identifier
# for clarity and expand the charset only if needed).
_COLLAPSE_RES = [
    re.compile(r"coalesce\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*'false'\s*\)", re.IGNORECASE),
    re.compile(r"coalesce\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*false\s*\)", re.IGNORECASE),
    re.compile(r"ifnull\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*false\s*\)", re.IGNORECASE),
    re.compile(r"nvl\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*0\s*\)", re.IGNORECASE),
    re.compile(r"coalesce\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*0\s*\)", re.IGNORECASE),
]

# (staging_filename_relative, bronze_col): reason. Reason must be non-empty.
# Entries here are Mode-5 patterns that are INTENTIONAL — the staging author
# declares "the distinction between NULL and False/0 is not semantically
# meaningful for this column" and justifies it in writing.
_INTENTIONAL_COALESCES: dict[tuple[str, str], str] = {
    # No intentional Mode 5 collapses after G4 landed. Add entries as
    # staging authors justify each one. Entries are only valid when the
    # source semantics are "NULL = absence = False" by convention
    # (e.g. StatsBomb's pass.cross tagging). For sources where NULL could
    # mean "unknown" (e.g. DFL is_cross where some matches may not record
    # it), prefer the nullable case expression pattern used in
    # stg_idsse__passes.sql for is_cross.
}


def test_no_silent_null_collapse_on_staging_booleans() -> None:
    violations: list[tuple[Path, str, int, str]] = []
    for sql in sorted(_STAGING_DIR.rglob("stg_*.sql")):
        rel = sql.relative_to(_STAGING_DIR.parent.parent)
        text = sql.read_text(encoding="utf-8")
        for line_no, raw_line in enumerate(text.splitlines(), start=1):
            stripped = raw_line.lstrip()
            if stripped.startswith("--"):
                continue
            for regex in _COLLAPSE_RES:
                for m in regex.finditer(raw_line):
                    col = m.group(1)
                    key = (str(rel).replace("\\", "/"), col)
                    if key in _INTENTIONAL_COALESCES:
                        continue
                    violations.append((rel, col, line_no, raw_line.rstrip()))

    assert not violations, (
        "Mode 5 (silent sentinel substitution) violations — these coalesce/ifnull/nvl\n"
        "patterns collapse NULL -> False/0 on staging columns:\n\n"
        + "\n".join(f"  {rel!s}:{lno}  [col={col}]\n      {line.strip()}" for rel, col, lno, line in violations)
        + "\n\nFix: use a nullable case expression:\n"
        "  case when X is null then null when X = 'true' then true else false end\n"
        "OR add (staging_path, col_name): 'reason' to _INTENTIONAL_COALESCES with a non-empty reason."
    )


def test_intentional_coalesces_have_reasons() -> None:
    """Every _INTENTIONAL_COALESCES entry must have a non-empty reason string."""
    empty_reasons = sorted(k for k, v in _INTENTIONAL_COALESCES.items() if not v)
    assert not empty_reasons, "_INTENTIONAL_COALESCES entries missing reason text:\n" + "\n".join(
        f"  {k}" for k in empty_reasons
    )
