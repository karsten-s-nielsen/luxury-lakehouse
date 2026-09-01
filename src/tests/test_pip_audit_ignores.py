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

from scripts.check_cve_blockers import NO_FIX, is_checkable
from scripts.pip_audit_ignores import REQUIRED_FIELDS, IgnoreListError, flags, load_ignores

_REPO = Path(__file__).resolve().parents[2]
_CI = _REPO / ".github" / "workflows" / "python-ci.yml"
_IGNORES = _REPO / ".pip-audit-ignores.yml"

#: Entries whose claim ("taking this fix is not free") the scheduled gate re-tests weekly.
_EXPECTED_CHECKABLE_IDS = {
    # Reported by the BASE resolution — what `uv run pip-audit` has always audited.
    "CVE-2026-28684",  # python-dotenv — taipy-gui caps <=1.0.1
    "PYSEC-2026-113",  # pyarrow — taipy-core caps <19.0
    "CVE-2025-58367",  # deepdiff — taipy-common caps <=7.0.1
    "CVE-2026-33155",  # deepdiff — same pair, listed by CVE alias
    "PYSEC-2026-3447",  # setuptools — torch caps <82; cu128 index tops out at torch 2.11.0
    # Reported ONLY by the taipy-app resolution (scripts/audit_resolutions.py, 2026-08-11).
    "PYSEC-2026-1383",  # flask-cors — taipy-gui caps <5.1
    "PYSEC-2026-1384",  # flask-cors — same cap
    "PYSEC-2026-1385",  # flask-cors — same cap
    "PYSEC-2026-89",  # markdown — taipy-gui caps <=3.6
    "PYSEC-2026-1605",  # marshmallow — taipy-rest caps <=3.21.2
    "PYSEC-2026-160",  # twisted — taipy-gui caps <24.8.0
    "PYSEC-2025-194",  # torch — the cu128 index publishes nothing above 2.11.0
    "PYSEC-2026-3716",  # datasets — taipy-core caps pyarrow <19.0; datasets 5.0.1 needs >=21.0.0
}

#: Entries with NO patched release upstream. The probe cannot test these; each must name the
#: awaited release in its review_trigger. Keep this set as small as the evidence allows.
#:
#: PYSEC-2026-3081 is the ONLY one, and it is here because upstream fixed it in commit 129fd40
#: and never cut a release — verified by reading `get_resource` out of taipy-gui 4.1.2, one
#: version ABOVE our pin, which still carries the flawed check. A version bump cannot retire it.
_EXPECTED_UNFIXABLE_IDS: set[str] = {"PYSEC-2026-3081"}


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


def test_every_ignore_is_either_checkable_or_declared_unfixable() -> None:
    """The scheduled blocker gate must cover a known set — and a parametrised gate with zero
    cases passes while asserting nothing.

    Pinned as SETS, both directions, not counts: a count permits swapping one entry out and
    another in, net unchanged, silently. Same shape as `_ALLOWED_BARE` in
    test_workspace_client_construction.py.

    The NO_FIX side is the one worth guarding. It is the only way to hold an ignore that the
    scheduled probe never tests, so adding one must be a visible, reviewed change rather than
    a quiet opt-out — the same reasoning that put an expiry on the bronze non-contract set
    (ADR-075).
    """
    entries = load_ignores()
    checkable = {str(e["id"]) for e in entries if is_checkable(e)}
    unfixable = {str(e["id"]) for e in entries if str(e["fix_in"]).strip() == NO_FIX}

    assert checkable == _EXPECTED_CHECKABLE_IDS, (
        f"drift: only-file={sorted(checkable - _EXPECTED_CHECKABLE_IDS)} "
        f"only-expected={sorted(_EXPECTED_CHECKABLE_IDS - checkable)}"
    )
    assert unfixable == _EXPECTED_UNFIXABLE_IDS, (
        f"drift: only-file={sorted(unfixable - _EXPECTED_UNFIXABLE_IDS)} "
        f"only-expected={sorted(_EXPECTED_UNFIXABLE_IDS - unfixable)}"
    )
    assert checkable, "no entry is checkable — the scheduled blocker gate would cover nothing"
    assert checkable | unfixable == {str(e["id"]) for e in entries}, (
        "an entry is neither checkable nor declared unfixable — partition, not filter"
    )


def test_the_unfixable_sentinel_is_spelled_exactly() -> None:
    """A near-miss (`none`, `None`, `n/a`) silently becomes a checkable entry with a garbage
    floor, which the probe then reports as UNKNOWN once a week forever."""
    for entry in load_ignores():
        fix = str(entry["fix_in"]).strip()
        assert fix == NO_FIX or fix.lower() not in {"none", "n/a", "unknown", "-", "tbd"}, (
            f"{entry['id']}: fix_in {fix!r} looks like the no-fix sentinel but is not {NO_FIX!r}"
        )


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
