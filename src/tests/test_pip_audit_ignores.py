"""The pip-audit ignore list is a single, structured, reviewable source (SEC5).

BEFORE: 16 `--ignore-vuln` flags inline in python-ci.yml, DUPLICATED across the
"Audit dependencies" and "Generate SBOM (CycloneDX)" steps, with their rationale in a
detached comment block above them. Nothing kept the two lists in sync — adding an ignore
to one and not the other turned CI red a second time on 2026-08-07 — and nobody could
tell which ignores were still blocked upstream and which had become resolvable months ago.
That is how the open-alert count reached 18.

AFTER: `.pip-audit-ignores.yml` is the single source; `scripts/pip_audit_ignores.py`
renders it for both steps. Every entry carries a `review_trigger`, because an ignore with
no stated condition for removal is one nobody can ever retire.

These tests enforce the properties that make that true, rather than trusting convention.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from scripts.pip_audit_ignores import REQUIRED_FIELDS, IgnoreListError, flags, load_ignores

_REPO = Path(__file__).resolve().parents[2]
_CI = _REPO / ".github" / "workflows" / "python-ci.yml"
_IGNORES = _REPO / ".pip-audit-ignores.yml"


def test_ignore_list_parses_and_is_complete() -> None:
    """Every entry carries all six fields, non-blank, with no duplicate advisory ids."""
    entries = load_ignores()
    assert entries, "ignore list is empty"
    for entry in entries:
        for field in REQUIRED_FIELDS:
            assert str(entry.get(field, "")).strip(), f"{entry.get('id')!r} missing/blank {field!r}"


def test_no_inline_ignore_flags_remain_in_ci() -> None:
    """CI must source ignores from the file — an inline flag re-opens the drift class.

    This is the assertion that actually prevents the 2026-08-07 recurrence: a developer
    adding `--ignore-vuln` to one step and not the other now fails here first.
    """
    ci = _CI.read_text(encoding="utf-8")
    assert "--ignore-vuln" not in ci, (
        "python-ci.yml contains inline --ignore-vuln flag(s). Add the advisory to "
        ".pip-audit-ignores.yml instead — that file is the single source both pip-audit "
        "steps read, and inline flags are exactly the duplication SEC5 removed."
    )


def test_both_pip_audit_steps_use_the_generator() -> None:
    """Both invocations must render from the same source; one bypassing it is the old bug."""
    ci = _CI.read_text(encoding="utf-8")
    invocations = re.findall(r"uv run pip-audit[^\n]*", ci)
    assert len(invocations) >= 2, f"expected the audit + SBOM invocations, found {invocations}"
    for line in invocations:
        assert "scripts/pip_audit_ignores.py" in line, (
            f"pip-audit invocation does not source the shared ignore list: {line!r}"
        )


def test_every_ignore_has_an_actionable_review_trigger() -> None:
    """A review_trigger must name a CONDITION, not restate the problem.

    Without this an ignore is permanent by default. Rejects the degenerate forms that
    would otherwise pass the non-blank check.
    """
    useless = {"none", "n/a", "tbd", "unknown", "never", "-"}
    for entry in load_ignores():
        trigger = str(entry["review_trigger"]).strip()
        assert trigger.lower() not in useless, f"{entry['id']}: review_trigger {trigger!r} is not actionable"
        assert len(trigger) > 15, f"{entry['id']}: review_trigger {trigger!r} is too vague to act on"


def test_flags_render_one_pair_per_entry() -> None:
    """The rendered flag list must match the file exactly — no silent drops."""
    entries = load_ignores()
    rendered = flags(entries)
    assert rendered.count("--ignore-vuln") == len(entries)
    for entry in entries:
        assert str(entry["id"]) in rendered


def test_malformed_entry_fails_loudly(tmp_path: Path) -> None:
    """A blank required field must raise, not silently drop the ignore.

    A dropped ignore turns CI red for a reason that looks unrelated to the edit that
    caused it — the failure mode this whole change exists to remove.
    """
    bad = tmp_path / "bad.yml"
    bad.write_text(
        yaml.safe_dump(
            {
                "ignores": [
                    {
                        "id": "CVE-1",
                        "package": "x",
                        "fix_in": "1",
                        "blocked_by": "y",
                        "justification": "z",
                        "review_trigger": "",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(IgnoreListError, match="missing/blank"):
        load_ignores(bad)


def test_duplicate_advisory_fails_loudly(tmp_path: Path) -> None:
    """Two entries for one advisory means one rationale is dead text nobody maintains."""
    entry = {
        "id": "CVE-1",
        "package": "x",
        "fix_in": "1",
        "blocked_by": "y",
        "justification": "z",
        "review_trigger": "an upstream release fixing it",
    }
    dup = tmp_path / "dup.yml"
    dup.write_text(yaml.safe_dump({"ignores": [entry, dict(entry)]}), encoding="utf-8")
    with pytest.raises(IgnoreListError, match="duplicate"):
        load_ignores(dup)
