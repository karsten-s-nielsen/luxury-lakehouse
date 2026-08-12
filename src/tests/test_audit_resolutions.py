"""Auditing every RESOLUTION, not just the environment CI happens to install.

`pyproject.toml` declares ``taipy-app`` / ``dbt`` / ``sdk`` as CONFLICTING extras, so no single
environment can hold them all — uv writes a forked lock instead. That is exactly why an
installed-environment audit cannot cover this project: for three of the four resolutions there
is no environment to install. Measured 2026-08-11, the base environment reported 7 findings
across 5 packages while the ``taipy-app`` resolution — the one the Space actually deploys —
reported **17 across 11**.

The resolver work is not exercised here (that is the workflow's job, minutes per resolution).
What IS exercised is the pure core, plus the two properties whose failure would make the whole
audit dishonest:

* the local-version rewrite must be REPORTED, never silent — it substitutes what is audited
* the resolution list must stay a partition of what we ship, not a subset someone trimmed
"""

from __future__ import annotations

import inspect
import json
import re
import subprocess
from pathlib import Path

import pytest

from scripts.audit_resolutions import (
    RESOLUTIONS,
    SPACE_REPOS,
    Outcome,
    _export_cmd,
    audit,
    bound_diagnostics,
    classify_audit,
    known_targets,
    label,
    main,
    strip_local_versions,
)

_REPO = Path(__file__).resolve().parents[2]


class TestStripLocalVersions:
    """A PEP 440 local version exists on no index, so PyPI cannot audit it.

    Under ``--disable-pip`` that costs the ONE package — ``Dependency not found on PyPI and
    could not be audited: torch (2.11.0+cu128)``, which ``--strict`` still turns into a failed
    run (measured 2026-08-12). Before ``--disable-pip`` it aborted the ENTIRE audit at the
    dry-run install. The blast radius shrank; the need to strip did not.
    """

    def test_the_real_torch_pin_is_rewritten(self) -> None:
        out, notes = strip_local_versions("torch==2.11.0+cu128\nnumpy==1.26.4\n")
        assert "torch==2.11.0\n" in out
        assert "+cu128" not in out
        assert notes == ["torch==2.11.0+cu128 -> torch==2.11.0"]

    def test_every_substitution_is_reported(self) -> None:
        """A security gate that silently substitutes its input is the failure mode this repo
        keeps paying for. The caller prints these on every run."""
        _, notes = strip_local_versions("a==1.0+abc\nb==2.0+def\n")
        assert len(notes) == 2

    def test_plain_pins_are_untouched(self) -> None:
        text = "numpy==1.26.4\npandas==2.2.2\n"
        out, notes = strip_local_versions(text)
        assert out == text
        assert notes == []

    def test_environment_markers_survive(self) -> None:
        """Dropping a marker would audit a package on platforms we never ship it to."""
        out, _ = strip_local_versions('torch==2.11.0+cu128 ; python_version < "3.11"\n')
        assert out == 'torch==2.11.0 ; python_version < "3.11"\n'

    def test_a_version_containing_no_local_segment_is_left_alone(self) -> None:
        out, notes = strip_local_versions("torch==2.11.0\n")
        assert out == "torch==2.11.0\n"
        assert notes == []

    def test_the_rewrite_is_generic_not_torch_specific(self) -> None:
        """Hard-coding torch would let the next local-versioned package fail as a mystery."""
        out, notes = strip_local_versions("some-pkg==1.2.3+localbuild\n")
        assert out == "some-pkg==1.2.3\n"
        assert notes == ["some-pkg==1.2.3+localbuild -> some-pkg==1.2.3"]


class TestResolutionCoverage:
    def test_every_conflicting_extra_is_audited(self) -> None:
        """The resolutions must cover what we ship. `taipy-app` is a superset of the others
        TODAY (measured: dbt and sdk contributed zero new advisory ids), but auditing only it
        would make a future dbt- or sdk-only advisory invisible by construction.

        Derived from pyproject's declared conflicts rather than hard-coded, so adding a fourth
        conflicting extra fails here instead of silently going unaudited.

        Compared against the RAW extras, not against ``label()``. ``export_resolution`` builds
        ``--extra <extra>``, so the extra string is what actually reaches uv; ``label()`` is
        display-only (``extra or "base"``). Asserting against labels passes only because
        ``label()`` is identity for every non-None value, so renaming one for readability would
        fail this test over a change that broke nothing.

        This is the ONLY coverage assertion of its kind. A second copy belongs in no other class:
        `TestDevGroupExclusion` asserts the export FLAG, which is a different property, and two
        copies that disagree on their comparison basis leave a reader unable to tell which one is
        authoritative on the day either fails.
        """
        pyproject = (_REPO / "pyproject.toml").read_text(encoding="utf-8")
        conflicts = re.search(r"conflicts = \[(.*?)\n\]", pyproject, re.DOTALL)
        assert conflicts is not None, "pyproject no longer declares [tool.uv] conflicts"
        declared = set(re.findall(r'extra = "([^"]+)"', conflicts.group(1)))
        # A regex that matches an empty block would make every assertion below vacuously true —
        # the way this test would quietly stop testing anything at all.
        assert declared, "the conflicts block parsed but yielded no extras — the regex is stale"
        unaudited = sorted(declared - set(RESOLUTIONS))
        assert not unaudited, f"conflicting extras never audited: {unaudited}"

    def test_the_base_resolution_is_included(self) -> None:
        """Base is what the installed-environment audit already covers; keeping it here means
        one command reproduces the whole picture locally."""
        assert None in RESOLUTIONS
        assert label(None) == "base"

    def test_resolution_labels_are_unique(self) -> None:
        labels = [label(r) for r in RESOLUTIONS]
        assert len(labels) == len(set(labels)), labels


_CLEAN_REPORT = '{"dependencies": [{"name": "flask", "version": "3.1.3", "vulns": []}], "fixes": []}'
_VULNERABLE_REPORT = (
    '{"dependencies": [{"name": "flask", "version": "3.1.1", "vulns": [{"id": "PYSEC-2026-2151"}]}], "fixes": []}'
)


class TestAuditClassification:
    """A failure to RUN must never be reported as vulnerabilities FOUND.

    On 2026-08-11 a FileNotFoundError in the project build made the job print
    `FAIL: unignored findings in 4 resolution(s): base, taipy-app, dbt, sdk` — four fabricated
    CVE regressions. This is the same BLOCKED/UNKNOWN rule check_cve_blockers.py already applies.
    """

    def test_clean_audit_is_clean(self) -> None:
        assert classify_audit(0, _CLEAN_REPORT).outcome is Outcome.CLEAN

    def test_findings_are_findings(self) -> None:
        assert classify_audit(1, _VULNERABLE_REPORT).outcome is Outcome.FINDINGS

    def test_output_that_is_not_json_is_unknown(self) -> None:
        """The audit did not run. Reporting this as FINDINGS cries wolf; as CLEAN it certifies
        a claim nothing tested."""
        result = classify_audit(1, "FileNotFoundError: Forced include not found: ...")
        assert result.outcome is Outcome.UNKNOWN
        assert "did not produce a JSON report" in result.detail

    def test_zero_exit_with_unparseable_output_is_also_unknown(self) -> None:
        """Exit code alone decides nothing — the report is the evidence."""
        assert classify_audit(0, "").outcome is Outcome.UNKNOWN

    def test_no_findings_but_nonzero_exit_is_unknown(self) -> None:
        """Self-contradictory: the shape a partial --strict collection failure takes. Reading it
        as CLEAN certifies a set pip-audit is saying it could not fully assess."""
        assert classify_audit(1, _CLEAN_REPORT).outcome is Outcome.UNKNOWN

    def test_a_truncated_package_list_says_so(self) -> None:
        """Same rule as the diagnostic bound: truncate if you must, never silently. The count is
        always exact; only the sample of names is capped."""
        deps = [{"name": f"pkg{i}", "version": "1.0", "vulns": [{"id": "X"}]} for i in range(9)]
        result = classify_audit(1, json.dumps({"dependencies": deps, "fixes": []}))
        assert result.detail.startswith("9 package(s) with unignored advisories:")
        assert "and 4 more" in result.detail

    def test_outcomes_format_and_compare_consistently(self) -> None:
        """A ``str``-mixin enum on 3.10 gives `Outcome.CLEAN` from str() and `CLEAN` from an
        f-string unless __str__ is overridden. The summary prints these into a fixed-width
        column, so the two must agree."""
        assert str(Outcome.CLEAN) == "CLEAN"
        assert f"{Outcome.UNKNOWN:9s}" == "UNKNOWN  "
        assert Outcome.FINDINGS == "FINDINGS"


class TestDiagnosticsAreRetained:
    """The evidence must survive the subprocess boundary.

    Dropping stderr in `audit()` is irreversible; declining to print it is a policy the caller
    can change. An UNKNOWN reading "pip-audit did not produce a JSON report (exit 1)" with the
    traceback discarded tells the reader to fix the runner while withholding the only evidence
    for doing so.
    """

    def test_stderr_never_changes_the_verdict(self) -> None:
        """Diagnostics are evidence for humans, NEVER input to the verdict.

        Matching prose would make a security gate depend on an upstream tool's wording, which is
        not an API (spec D2). The one stderr signal that must count — a dependency that could not
        be collected — already reaches us structurally, as a non-zero exit under --strict.
        """
        noise = "ERROR: everything is on fire\nNo solution found when resolving dependencies"
        for code, out in ((0, _CLEAN_REPORT), (1, _VULNERABLE_REPORT), (1, "not json"), (1, _CLEAN_REPORT)):
            assert classify_audit(code, out, "").outcome is classify_audit(code, out, noise).outcome

    def test_stderr_is_carried_on_an_unknown(self) -> None:
        result = classify_audit(1, "boom", "Traceback (most recent call last):\n  FileNotFoundError")
        assert "FileNotFoundError" in result.diagnostics

    def test_unparseable_stdout_is_evidence_too(self) -> None:
        """When stdout is not a report it is the other half of what the tool said. Keeping only
        stderr would discard it."""
        result = classify_audit(1, "Forced include not found: dbt_packages", "")
        assert "Forced include not found" in result.diagnostics
        assert "not a JSON report" in result.diagnostics

    def test_a_parseable_report_is_not_dumped_into_diagnostics(self) -> None:
        """On a parseable run stdout IS the report; adding it here would bury the diagnostics."""
        result = classify_audit(0, _CLEAN_REPORT, "some progress output")
        assert "dependencies" not in result.diagnostics
        assert "some progress output" in result.diagnostics


class TestDevGroupExclusion:
    """Dev tooling is python-ci.yml's surface, not this job's.

    Measured 2026-08-11: `uv export --extra taipy-app` yields 237 packages, with
    --no-default-groups 141. The 96-package delta is the dev group. Auditing it here made torch
    and setuptools advisories read as production exposure when the Space contains neither.

    No coverage assertion lives in this class — `TestResolutionCoverage` owns that property, and
    a second copy of it was merged away rather than added (see that test's docstring).
    """

    def test_every_export_excludes_default_groups(self) -> None:
        for extra in RESOLUTIONS:
            assert "--no-default-groups" in _export_cmd(extra), f"{label(extra)} would include dev"

    def test_the_flag_is_unconditional(self) -> None:
        """Anti-drift, asserted on BEHAVIOUR and SIGNATURE — never by scraping the source.

        The flag must not depend on the argument (so exhaustive inputs, including an invented
        extra), and no second parameter may exist for it to depend on (so a future
        `_export_cmd(extra, include_dev=False)` cannot reopen the defect while the loop above
        still passes).
        """
        for extra in (None, "taipy-app", "dbt", "sdk", "not-a-real-extra"):
            assert "--no-default-groups" in _export_cmd(extra), f"omitted for {extra!r}"
        params = inspect.signature(_export_cmd).parameters
        assert list(params) == ["extra"], f"unexpected parameters: {list(params)}"

    def test_no_dev_target_is_defined(self) -> None:
        """B3, the other half of this decision: python-ci.yml audits the installed env on EVERY
        PR — 216 packages with markers evaluated for linux — and all four forks' dev-side
        packages are inside it, 0 uncovered, measured 2026-08-11. A dev target here would add no
        coverage and would give one advisory two owners.

        Lives here rather than in TestResolutionCoverage because it is not a coverage claim: it
        asserts an ABSENCE that the exclusion above depends on for its justification.
        """
        assert not [r for r in RESOLUTIONS if r and "dev" in r]


class TestAuditNeverResolves:
    """The gate audits a LOCKED file; it must never dry-run-install it to find out what is in it.

    pip-audit 2.10.1 gates its venv-free path on ``--disable-pip``
    (``_dependency_source/requirement.py:161``); ``--no-deps`` only AUTHORIZES that flag and does
    not imply it. Without it pip-audit builds a throwaway venv and installs the whole resolution
    into it — which is how `cve-blocker-review.yml` failed on EVERY target from the day it was
    created: the venv's ``ensurepip --upgrade --default-pip`` exits 1 under the uv-managed CPython
    on the runner, so all four resolutions classified UNKNOWN and the gate never once reached a
    verdict. `audit()`'s own docstring already claimed this behaviour ("audits the pinned lines as
    given ... re-resolving would both be slower and audit versions we do not ship"); the argv did
    not implement it.

    Measured 2026-08-12, `sdk` resolution: **36s -> 1s**. The audited set is unchanged — base
    resolution yields 55 dependencies, 0 skipped, and the same PYSEC-2026-3552 on cryptography
    49.0.0 either way. Environment markers are still evaluated on the venv-free path
    (``requirement.py:319``), so the marker semantics `audit()` documents survive.
    """

    def test_the_audit_never_invokes_pips_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Asserted on the argv actually handed to the subprocess, not on the source text."""
        captured: list[list[str]] = []

        def _fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            captured.append(list(cmd))
            return subprocess.CompletedProcess(
                cmd, 0, '{"dependencies": [{"name": "x", "version": "1", "vulns": []}]}', ""
            )

        monkeypatch.setattr(subprocess, "run", _fake_run)
        audit(Path("requirements.txt"))

        assert captured, "audit() did not invoke a subprocess at all"
        assert "--disable-pip" in captured[0], (
            f"pip-audit will dry-run-install the resolution into a throwaway venv: argv={captured[0]}"
        )


class TestDeployedSpaceTarget:
    """Audit what is DEPLOYED, not a fresh resolve.

    `hf_taipy_app/requirements.txt` is generated at deploy time by `uv pip compile`, which always
    takes the newest satisfying release — so two deploys of the SAME commit can ship different
    versions. Re-compiling at audit time would certify a pin set that may never have existed in
    production. `manage_space.py` uploads the folder via `upload_folder` and `requirements.txt` is
    not in IGNORE_PATTERNS, so the real pin set is one download away.

    Nothing else observes this surface: no dependabot ecosystem covers `hf_taipy_app/`, its
    requirements file is gitignored *specifically* to hide it from dependabot-pip, and `uv.lock`
    is a different resolution.
    """

    def test_both_spaces_are_targeted(self) -> None:
        ids = dict(SPACE_REPOS)
        assert ids["space-production"] == "luxury-lakehouse/soccer-analytics-app"
        assert ids["space-staging"] == "luxury-lakehouse/staging"

    def test_space_ids_match_manage_space(self) -> None:
        """The repo ids live in manage_space.py. Hard-coding a second copy that drifts is how an
        audit silently starts watching a Space that no longer exists."""
        src = (_REPO / "scripts" / "manage_space.py").read_text(encoding="utf-8")
        for _, repo_id in SPACE_REPOS:
            assert repo_id in src, f"{repo_id} not found in manage_space.py"

    def test_only_accepts_a_space_name(self) -> None:
        """`--only space-production` must be a legal target, not an `unknown target` exit.

        The Spaces are not in RESOLUTIONS, so the pre-existing filter rejected them.
        """
        assert "space-production" in known_targets()
        assert "space-staging" in known_targets()
        for extra in RESOLUTIONS:
            assert label(extra) in known_targets()

    def test_an_unknown_only_is_still_rejected(self) -> None:
        """A typo'd --only must ERROR, never audit an empty set and exit 0.

        A gate that passes by auditing nothing is worse than one that fails loudly.
        """
        assert main(["--only", "not-a-target"]) == 1


class TestBoundDiagnostics:
    def test_short_output_is_untouched(self) -> None:
        assert bound_diagnostics("a\nb\nc") == "a\nb\nc"

    def test_both_ends_survive_and_the_elision_is_announced(self) -> None:
        """A resolver error leads with its summary; a traceback ends with the exception type.
        Keeping one end would lose the answer for one of the two failures this gate sees.

        The count is stated because a size bound is itself something that can hide the answer —
        a silent truncation would be a quieter version of the bug retention fixes.
        """
        text = "\n".join(f"line{i}" for i in range(100))
        out = bound_diagnostics(text, head=3, tail=2)
        assert out.startswith("line0\nline1\nline2\n")
        assert out.endswith("line98\nline99")
        assert "... 95 line(s) omitted ..." in out

    def test_the_boundary_case_keeps_everything(self) -> None:
        text = "\n".join(f"line{i}" for i in range(5))
        assert bound_diagnostics(text, head=3, tail=2) == text
