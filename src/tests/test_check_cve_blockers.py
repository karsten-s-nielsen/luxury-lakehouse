"""The CVE-blocker probe: pure-core behaviour, and the four outcomes it must keep distinct.

Enforces the ADR-075 amendment: a revisit condition that only a human can evaluate is still
unobserved, so where it can be evaluated by EXECUTION, it must be.

``scripts/check_cve_blockers.py`` asserts what each pip-audit ignore CLAIMS — that taking the
fix is not free — by attempting the floor and requiring the resolve to fail, or to succeed only
by moving other packages.

The resolver itself is not exercised here (that is the scheduled workflow's job). What IS
exercised is every way the probe could report a verdict it did not earn, because each of those
turns a security gate into decoration:

* a failure for a NON-resolution reason (offline runner, corrupt cache) read as ``BLOCKED``
* a resolve bought with downgrades read as ``MOVED`` — measured on both entries that resolve,
  one of which pays for it by re-opening three other open alerts
* a constraint satisfied by the target LEAVING the graph, which no pre-flight check can see
* a constraint on a package absent from the lock — inert, resolves cleanly, reads as ``MOVED``
* the injected constraint landing somewhere the resolver never reads
* the probe running against a dirty tree, where its own restore destroys real work
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from scripts import check_cve_blockers
from scripts.check_cve_blockers import (
    BLOCKED,
    COLLATERAL,
    MOVED,
    NO_FIX,
    UNKNOWN,
    ProbeError,
    Result,
    classify,
    format_failure,
    graph_changes,
    inject_constraint,
    is_checkable,
    lock_versions,
    locked_packages,
    normalize,
    package_name,
    resolver_explanation,
)

_REPO = Path(__file__).resolve().parents[2]

_MINIMAL_PYPROJECT = """[project]
name = "demo"

[tool.uv]
constraint-dependencies = [
    "pyasn1>=0.6.4",
]
"""


def _entry(**over: str) -> dict[str, str]:
    base = {
        "id": "CVE-1",
        "package": "demo 1.0.0",
        "fix_in": "2.0.0",
        "blocked_by": "holder caps demo <2",
        "justification": "j",
        "review_trigger": "a holder release relaxing the cap",
    }
    base.update(over)
    return base


class TestOutcomeClassification:
    """BLOCKED / COLLATERAL / MOVED / UNKNOWN must stay four things."""

    def test_a_clean_resolve_with_no_collateral_means_the_blocker_moved(self) -> None:
        assert classify(0, "", collateral=[])[0] == MOVED

    def test_unsatisfiable_resolve_means_the_blocker_holds(self) -> None:
        outcome, _ = classify(1, "  x No solution found when resolving dependencies:\n  ...")
        assert outcome == BLOCKED

    def test_a_failure_that_is_not_unsatisfiability_is_unknown(self) -> None:
        """The one that matters: an offline runner must not certify every blocker as intact.

        A gate that reads "uv exited non-zero" as "the cap still binds" reports a clean bill of
        health from a machine with no network — the loudest possible false green.
        """
        outcome, detail = classify(2, "error: Failed to fetch: https://pypi.org/simple/demo/")
        assert outcome == UNKNOWN
        assert "not unsatisfiability" in detail

    def test_a_resolve_bought_with_downgrades_is_not_a_moved_blocker(self) -> None:
        """Measured 2026-08-11 on the real lock, and the reason this outcome exists.

        `cryptography>=50.0.0` resolves — by taking mlflow 3.15.1 -> 3.2.0 and flask-cors
        6.0.2 -> 5.0.1, the latter re-opening three other open Dependabot alerts. Reporting
        that as "the blocker has moved, apply the floor" is a false alarm that also happens
        to be a security regression.
        """
        outcome, detail = classify(0, "", collateral=["mlflow: 3.15.1 -> 3.2.0"])
        assert outcome == COLLATERAL
        assert "only by changing 1 other package" in detail

    def test_a_target_that_leaves_the_lock_did_not_get_floored(self) -> None:
        """`setuptools>=83.0.0` resolves by backtracking torch to a release that does not
        depend on setuptools at all — so the constraint is satisfied by the package's ABSENCE.

        A pre-flight presence check cannot catch this: the package is there when the probe
        starts and gone when it ends.
        """
        outcome, detail = classify(0, "", collateral=["torch: 2.11.0 -> 2.10.0"], target_vanished=True)
        assert outcome == COLLATERAL
        assert "LEFT the lock" in detail and "vacuously" in detail

    def test_both_blocking_modes_pass_and_the_rest_fail(self) -> None:
        assert Result("a", "p", "f", BLOCKED, "d").ok
        assert Result("a", "p", "f", COLLATERAL, "d").ok, "a fix with a real cost is still blocked"
        assert not Result("a", "p", "f", MOVED, "d").ok
        assert not Result("a", "p", "f", UNKNOWN, "d").ok, "UNKNOWN must fail, not shrug"


class TestGraphChanges:
    """The diff that separates 'the blocker moved' from 'the fix costs something'."""

    def test_the_target_is_excluded_from_its_own_collateral(self) -> None:
        lines, vanished = graph_changes({"cryptography": ("49.0.0",)}, {"cryptography": ("50.0.0",)}, "cryptography")
        assert lines == []
        assert not vanished

    def test_other_packages_moving_is_collateral(self) -> None:
        lines, _ = graph_changes(
            {"cryptography": ("49.0.0",), "mlflow": ("3.15.1",)},
            {"cryptography": ("50.0.0",), "mlflow": ("3.2.0",)},
            "cryptography",
        )
        assert lines == ["mlflow: 3.15.1 -> 3.2.0"]

    def test_removals_and_additions_are_collateral(self) -> None:
        """An addition and a removal must not render identically — `REMOVED -> 3` reads as the
        opposite of what happened, in a report whose whole job is saying what the fix costs."""
        lines, _ = graph_changes({"x": ("1",), "gone": ("2",)}, {"x": ("1",), "new": ("3",)}, "x")
        assert lines == ["gone: 2 -> REMOVED", "new: ABSENT -> 3"]

    def test_a_vanished_target_is_flagged_separately(self) -> None:
        _, vanished = graph_changes({"setuptools": ("81.0.0",)}, {}, "setuptools")
        assert vanished

    def test_target_matching_is_pep503_normalised(self) -> None:
        """`Twisted` in the entry vs `twisted` in the lock must be one package, or the target
        gets counted as its own collateral and every probe reads as COLLATERAL."""
        lines, vanished = graph_changes({"twisted": ("25.5.0",)}, {"twisted": ("26.4.0",)}, "Twisted")
        assert lines == []
        assert not vanished

    def test_a_change_confined_to_one_fork_is_still_collateral(self) -> None:
        """The defect a `{name: version}` map hid.

        `flask-cors` is pinned at BOTH 5.0.1 and 6.0.2 in the real lock (the taipy-app / dbt /
        sdk extras are declared as conflicting, so uv forks the resolution). A last-wins map
        compares one arbitrary fork and reports nothing changed when the other fork moved.
        """
        lines, _ = graph_changes({"flask-cors": ("5.0.1", "6.0.2")}, {"flask-cors": ("5.0.1",)}, "cryptography")
        assert lines == ["flask-cors: 5.0.1, 6.0.2 -> 5.0.1"]

    def test_a_forked_package_that_did_not_move_is_not_collateral(self) -> None:
        lines, _ = graph_changes({"pyarrow": ("18.1.0", "19.0.1")}, {"pyarrow": ("18.1.0", "19.0.1")}, "cryptography")
        assert lines == []


class TestResolverExplanation:
    """uv names the cap holder every time it refuses. Capture it — the hand-written
    `blocked_by` does not stay true on its own.

    Audited 2026-08-11: three of six entries named the wrong package, and one named two
    packages (`databricks-connect`, `pyspark`) that are not in uv.lock at all — so its
    `review_trigger` watched an upstream that could never have unblocked it.
    """

    _REAL = (
        "\x1b[31m  x\x1b[0m No solution found when resolving dependencies for split (included:\n"
        "  | luxury-lakehouse[sdk], luxury-lakehouse[taipy-app]; excluded:\n"
        "  | luxury-lakehouse[dbt]):\n"
        "  `-> Because taipy-gui>=4.1.0 depends on markdown>=3.4.4,<=3.6 and\n"
        "      markdown>=3.8.1, we can conclude that taipy-gui>=4.1.0 cannot be used.\n"
    )

    def test_the_cap_holder_survives_into_the_captured_lines(self) -> None:
        lines = resolver_explanation(self._REAL)
        assert any("taipy-gui" in line and "markdown>=3.4.4,<=3.6" in line for line in lines)

    def test_ansi_colour_is_stripped(self) -> None:
        """The scheduled job's log gets pasted into a justification; escape codes must not."""
        assert not any("\x1b[" in line for line in resolver_explanation(self._REAL))

    def test_nothing_is_captured_when_there_was_no_resolution_failure(self) -> None:
        assert resolver_explanation("Resolved 307 packages in 2ms") == ()

    def test_capture_is_bounded(self) -> None:
        """Six entries x an unbounded chain would bury the verdict lines it explains."""
        assert len(resolver_explanation(self._REAL + "extra\n" * 50)) <= 8


class TestConstraintInjection:
    def test_constraint_lands_inside_the_uv_constraint_list(self) -> None:
        """Placement, not mere presence — a constraint written anywhere else is inert.

        This is the same failure Unit A hit splicing sources.yml: the file stayed valid and
        the tool reported success while the payload sat in the wrong block.
        """
        out = inject_constraint(_MINIMAL_PYPROJECT, "demo>=2.0.0")
        block = re.search(r"constraint-dependencies = \[(.*?)\]", out, re.DOTALL)
        assert block is not None
        assert "demo>=2.0.0" in block.group(1)

    def test_existing_constraints_survive(self) -> None:
        assert "pyasn1>=0.6.4" in inject_constraint(_MINIMAL_PYPROJECT, "demo>=2.0.0")

    def test_result_is_insert_only(self) -> None:
        after = inject_constraint(_MINIMAL_PYPROJECT, "demo>=2.0.0")
        it = iter(after.splitlines())
        assert all(any(a == b for a in it) for b in _MINIMAL_PYPROJECT.splitlines())

    def test_missing_section_raises_rather_than_appending_somewhere(self) -> None:
        with pytest.raises(ProbeError, match="constraint-dependencies"):
            inject_constraint("[project]\nname = 'demo'\n", "demo>=2.0.0")

    def test_the_real_pyproject_has_the_anchor(self) -> None:
        """A guard against silent inertness: if the section is ever renamed, fail HERE.

        Otherwise the scheduled job is the discovery mechanism, a week later.
        """
        assert inject_constraint((_REPO / "pyproject.toml").read_text(encoding="utf-8"), "demo>=1")


class TestPackageIdentity:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("Twisted", "twisted"), ("flask_cors", "flask-cors"), ("zope.interface", "zope-interface")],
    )
    def test_pep503_normalisation(self, raw: str, expected: str) -> None:
        assert normalize(raw) == expected

    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("python-dotenv (1.2.1 in the base resolution; the taipy-app extra caps it)", "python-dotenv"),
            ("pyarrow 19.0.1", "pyarrow"),
            ("cryptography", "cryptography"),
        ],
    )
    def test_name_is_the_leading_token_of_the_prose_field(self, field: str, expected: str) -> None:
        assert package_name(_entry(package=field)) == expected

    def test_blank_package_raises(self) -> None:
        with pytest.raises(ProbeError, match="blank package"):
            package_name(_entry(package="  "))

    def test_locked_packages_parses_the_real_lock(self) -> None:
        names = locked_packages((_REPO / "uv.lock").read_text(encoding="utf-8"))
        assert len(names) > 100, f"parsed only {len(names)} packages — the [[package]] regex broke"
        assert {"python-dotenv", "deepdiff", "cryptography"} <= names

    def test_lock_versions_reads_real_pinned_versions(self) -> None:
        """The collateral diff is version-to-version, so a name-only parse would report
        nothing ever changed and every probe would read as MOVED."""
        pinned = lock_versions((_REPO / "uv.lock").read_text(encoding="utf-8"))
        assert re.fullmatch(r"\d+\.\d+.*", pinned["cryptography"][0]), pinned["cryptography"]
        assert len(pinned) == len(locked_packages((_REPO / "uv.lock").read_text(encoding="utf-8")))

    def test_lock_versions_keeps_every_fork_of_a_forked_package(self) -> None:
        """Not a synthetic case: `pyproject.toml` declares taipy-app / dbt / sdk as conflicting
        extras, so uv writes a forked lock. Two of the six ignore entries name packages that
        appear twice in it, and a last-wins map would silently watch only one fork.
        """
        pinned = lock_versions((_REPO / "uv.lock").read_text(encoding="utf-8"))
        forked = {name: vs for name, vs in pinned.items() if len(vs) > 1}
        assert forked, "no forked package found — either the lock changed shape or the parse broke"
        assert len(pinned["python-dotenv"]) == 2, pinned["python-dotenv"]

    def test_a_package_absent_from_the_lock_is_not_reported_present(self) -> None:
        """An inert constraint resolves cleanly, which would read as MOVED — a false alarm
        that sends someone chasing an upstream release that never happened."""
        assert "no-such-package-xyz" not in locked_packages((_REPO / "uv.lock").read_text(encoding="utf-8"))


class TestCheckability:
    def test_an_entry_with_a_real_fix_is_checkable(self) -> None:
        assert is_checkable(_entry())

    def test_the_no_fix_sentinel_is_not_checkable(self) -> None:
        """There is no version to floor to; ``taipy>=NONE`` is not a requirement."""
        assert not is_checkable(_entry(fix_in=NO_FIX))

    def test_a_blank_fix_is_not_checkable(self) -> None:
        assert not is_checkable(_entry(fix_in="  "))


class TestDirtyTreeGuard:
    """The restore is ``git checkout --``, which is destructive by nature.

    It cannot leave a dirty tree after an aborted probe — but it also cannot tell the probe's
    own edit apart from someone's uncommitted work. The precondition is what makes "the
    restore is safe" a property rather than a habit.
    """

    @staticmethod
    def _fake_git(stdout: str) -> object:
        def _run(*_args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(args=["git"], returncode=0, stdout=stdout, stderr="")

        return _run

    def test_a_dirty_pyproject_refuses_to_probe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(check_cve_blockers, "_git", self._fake_git(" M pyproject.toml\n"))
        with pytest.raises(ProbeError, match="uncommitted changes"):
            check_cve_blockers.assert_probe_files_clean()

    def test_a_clean_tree_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(check_cve_blockers, "_git", self._fake_git(""))
        check_cve_blockers.assert_probe_files_clean()

    def test_a_failed_restore_raises_rather_than_reporting_a_verdict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A silent restore failure leaves the probe's constraint in pyproject.toml."""

        def _run(*_args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(args=["git"], returncode=1, stdout="", stderr="boom")

        monkeypatch.setattr(check_cve_blockers, "_git", _run)
        with pytest.raises(ProbeError, match="failed to restore"):
            check_cve_blockers.restore_probe_files()


class TestFailureMessage:
    def test_a_moved_blocker_quotes_the_entry_and_names_the_action(self) -> None:
        """The next reader must not have to re-derive why the ignore existed."""
        entry = _entry()
        msg = format_failure(entry, Result("CVE-1", "demo", "demo>=2.0.0", MOVED, "resolved"))
        assert "NOW RESOLVES" in msg
        assert entry["blocked_by"] in msg
        assert entry["review_trigger"] in msg
        assert "ACTION" in msg

    def test_an_unknown_does_not_claim_the_blocker_moved(self) -> None:
        """UNKNOWN is loud but must not send someone to apply a floor that may not resolve."""
        msg = format_failure(_entry(), Result("CVE-1", "demo", "demo>=2.0.0", UNKNOWN, "offline"))
        assert "NO VERDICT" in msg
        assert "NOW RESOLVES" not in msg
        assert "ACTION" not in msg
