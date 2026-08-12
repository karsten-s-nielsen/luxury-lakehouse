"""A CI step that runs a wheel-importing script must install the wheel.

WHY THIS EXISTS
---------------
2026-08-11, on `main`: `terraform-apply.yml`'s post-apply "Patch job retry settings" step ran

    pip install -q "databricks-sdk==0.121.0" requests
    python scripts/patch_job_retries.py

while that script had just been changed to `from ingestion.databricks_auth import
workspace_client`. Terraform Apply itself succeeded; the step died with
`ModuleNotFoundError: No module named 'ingestion'`, so the ADR-025 job-retry patch never ran.

The hazard was even written down — ADR-075's own Consequences note that consolidating onto
`workspace_client()` widens `scripts/` -> `ingestion` coupling and "means those scripts need the
wheel installed". Nothing checked WHICH CI steps that applied to.

WHAT THE EXISTING GATE MISSED
-----------------------------
`test_workspace_client_construction.py` asserts CI-reachable scripts *import* the shared helper.
It never asserts their runtime can *resolve* it. That is a half-observed property, which is
worse than none: it looks covered. This module closes the other half.

A step's dependency declaration and its script's imports are two descriptions of the same
requirement, and nothing kept them in sync — the imports changed, the hand-rolled
`pip install databricks-sdk requests` did not.

NO ALLOWLIST BY DESIGN
----------------------
The rule is structural: a script that does not import a wheel package is simply not subject to
it. `patch_job_retries.py` satisfies the gate by going back to a bare client (it runs only under
`DATABRICKS_AUTH_TYPE=github-oidc`, where no `~/.databrickscfg` exists and the profile ambiguity
`workspace_client()` improves cannot arise) — recorded in `_ALLOWED_BARE` in the sibling module.
An allowlist here would let the next offender be waved through.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO / "scripts"
_WORKFLOWS = _REPO / ".github" / "workflows"

#: The five hatchling packages in the wheel. Importing any of them requires the project
#: environment; `scripts.*` and stdlib do not.
_WHEEL_PACKAGES = ("ingestion", "analytics", "shared", "workflows", "evolve")

#: `scripts/foo.py` or `scripts/sub/foo.py` as it appears in a workflow `run:` block.
_SCRIPT_REF_RE = re.compile(r"scripts/[\w/]+\.py")

#: A trailing shell comment inside a `run:` block. Anchored on start-of-line or whitespace
#: before the `#` so a URL fragment is NOT truncated — this repo pins wheels with
#: `...whl#sha256=…`, and a bare `#` split would mangle them.
_SHELL_COMMENT_RE = re.compile(r"(?:^|\s)#.*$")


def _imports_wheel_package(path: Path) -> set[str]:
    """Wheel packages this script imports, by AST.

    AST rather than text: a path string, a comment or a docstring mentioning `ingestion` is not
    an import, and this cycle has already been burned once classifying by substring.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return set()
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".")[0]
            if root in _WHEEL_PACKAGES:
                found.add(root)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in _WHEEL_PACKAGES:
                    found.add(root)
    return found


def _provides_project(step_run: str, run_commands: list[str]) -> bool:
    """Does this step execute with the project environment available?

    ``uv run`` materialises it, EXCEPT in two forms that withhold it:

    * ``--no-project`` deliberately does not build it (the weekly CVE workflow relies on that,
      and its scripts import only `scripts.*`).
    * ``--no-sync`` declines to sync one. It runs against whatever the environment already
      holds, so it provides the project only if some OTHER step actually installed it.

    Otherwise fall back to whether any step installs it. Two rules that each began as a bug:

    * ``uv sync --no-install-project`` is **not** an install — that flag is precisely "skip the
      editable install of this project". ``dbt-live-ci.yml:69`` uses it because the editable
      build force-includes ``dbt_project/dbt_packages``, which ``dbt deps`` does not create
      until a later step.
    * The fallback reads other steps' ``run:`` **commands**, never the workflow's raw text. A
      comment is prose: ``dbt-live-ci.yml:36`` mentions ``uv sync`` while describing a timeout
      budget, and on raw text that alone marked the workflow as having installed the project.

    Any one of these three rated the 2026-08-12 `upload_ci_shim.py` breakage green.
    """
    for line in step_run.splitlines():
        if "uv run" in line and "--no-project" not in line and "--no-sync" not in line:
            return True
    return any(
        "uv sync" in stripped and "--no-install-project" not in stripped
        for command in run_commands
        for line in command.splitlines()
        for stripped in (_SHELL_COMMENT_RE.sub("", line),)
    )


def _offenders() -> list[str]:
    """(workflow, script, packages) for every step that cannot import what it imports."""
    out: list[str] = []
    for workflow in sorted(_WORKFLOWS.glob("*.y*ml")):
        parsed = yaml.safe_load(workflow.read_text(encoding="utf-8")) or {}
        for job in (parsed.get("jobs") or {}).values():
            steps = job.get("steps") or []
            # The job's OWN run: commands — not the file's text. A `uv sync` inside a comment
            # is documentation, and reading it as an install is what hid dbt-live-ci.yml.
            commands = [s["run"] for s in steps if s.get("run")]
            for step in steps:
                run = step.get("run")
                if not run:
                    continue
                for ref in _SCRIPT_REF_RE.findall(run):
                    script = _REPO / ref
                    if not script.is_file():
                        continue
                    packages = _imports_wheel_package(script)
                    if packages and not _provides_project(run, commands):
                        out.append(f"{workflow.name} -> {ref} imports {sorted(packages)}")
    return sorted(set(out))


def test_ci_steps_can_import_what_their_scripts_import() -> None:
    """The failure this prevents is post-merge, on main, in a workflow no PR run exercises."""
    offenders = _offenders()
    assert not offenders, (
        "workflow step(s) run a script that imports a wheel package, without installing the "
        f"project: {offenders}. Either add `uv sync` / `uv run` to the step, or stop importing "
        "the wheel there (and record the reason in _ALLOWED_BARE if it is a client construction)."
    )


def test_the_detector_is_not_vacuous() -> None:
    """A detector that finds no scripts passes the assertion above while checking nothing."""
    referenced = {
        ref
        for workflow in _WORKFLOWS.glob("*.y*ml")
        for ref in _SCRIPT_REF_RE.findall(workflow.read_text(encoding="utf-8"))
        if (_REPO / ref).is_file()
    }
    assert len(referenced) >= 5, f"only {len(referenced)} workflow-referenced scripts found"
    importers = {r for r in referenced if _imports_wheel_package(_REPO / r)}
    assert importers, "no workflow-referenced script imports the wheel — the AST check is broken"


def test_no_project_is_not_treated_as_providing_the_project() -> None:
    """`uv run --no-project` deliberately skips the project install.

    Reading it as "uv is present, therefore fine" would silently re-open exactly this hole for
    the weekly CVE workflow, which uses that form on purpose.
    """
    assert not _provides_project("uv run --no-project --with pyyaml python scripts/x.py", [])
    assert _provides_project("uv run python scripts/x.py", [])
    assert _provides_project("python scripts/x.py", ["uv sync --frozen"])


class TestTheGuardCanActuallyFail:
    """The three ways this gate passed the very step it was written to catch.

    Shipped in #519 after `patch_job_retries.py` broke Terraform Apply. On 2026-08-12 the
    daily `dbt-live-ci.yml` died with `ModuleNotFoundError: No module named 'ingestion'` on
    `scripts/upload_ci_shim.py` — the identical defect, in a workflow this gate was already
    scanning, which it rated green. `--no-project` was treated as the only form that withholds
    the environment; it is not the only one.
    """

    def test_no_sync_does_not_provide_the_project(self) -> None:
        """`uv run --no-sync` runs against whatever exists and syncs nothing.

        This is the literal failing step: `uv run --no-sync python scripts/upload_ci_shim.py`.
        """
        assert not _provides_project("uv run --no-sync python scripts/upload_ci_shim.py", [])

    def test_sync_that_skips_the_project_is_not_an_install(self) -> None:
        """`--no-install-project` is the flag that means "do not install this project".

        `dbt-live-ci.yml:69` uses it deliberately: the editable build force-includes
        `dbt_project/dbt_packages`, which `dbt deps` does not create until a later step. Counting
        it as an install credits the workflow with a wheel that is provably absent.
        """
        assert not _provides_project(
            "uv run --no-sync python scripts/x.py",
            ["uv sync --frozen --extra dbt --no-install-project"],
        )

    def test_a_comment_mentioning_uv_sync_is_not_an_install(self) -> None:
        """The deepest of the three: the fallback matched the workflow's RAW TEXT.

        `dbt-live-ci.yml:36` is prose about a timeout budget — "the extra 10 minutes covers
        checkout, uv sync, tarball" — and it alone satisfied the old fallback. Documentation
        could silence the gate. The fallback now reads `run:` COMMANDS, so a comment cannot.
        """
        assert not _provides_project(
            "uv run --no-sync python scripts/x.py",
            ["# budget (120 * 15s); the extra 10 minutes covers checkout, uv sync, tarball"],
        )
