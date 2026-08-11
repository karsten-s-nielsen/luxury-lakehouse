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

import re
from pathlib import Path

from scripts.audit_resolutions import RESOLUTIONS, label, strip_local_versions

_REPO = Path(__file__).resolve().parents[2]


class TestStripLocalVersions:
    """``pip-audit -r`` dry-run-installs the file, so a PEP 440 local version that exists on
    no index aborts the ENTIRE audit — not just that package."""

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
        """
        pyproject = (_REPO / "pyproject.toml").read_text(encoding="utf-8")
        conflicts = re.search(r"conflicts = \[(.*?)\n\]", pyproject, re.DOTALL)
        assert conflicts is not None, "pyproject no longer declares [tool.uv] conflicts"
        declared = set(re.findall(r'extra = "([^"]+)"', conflicts.group(1)))
        audited = {label(r) for r in RESOLUTIONS}
        assert declared <= audited, f"conflicting extras never audited: {sorted(declared - audited)}"

    def test_the_base_resolution_is_included(self) -> None:
        """Base is what the installed-environment audit already covers; keeping it here means
        one command reproduces the whole picture locally."""
        assert None in RESOLUTIONS
        assert label(None) == "base"

    def test_resolution_labels_are_unique(self) -> None:
        labels = [label(r) for r in RESOLUTIONS]
        assert len(labels) == len(set(labels)), labels
