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


def _provides_project(step_run: str, workflow_text: str) -> bool:
    """Does this step execute with the project environment available?

    ``uv run`` materialises it — EXCEPT with ``--no-project``, which deliberately does not (the
    weekly CVE workflow relies on that, and its scripts import only `scripts.*`). A workflow
    containing ``uv sync`` has installed it for every later step.
    """
    for line in step_run.splitlines():
        if "uv run" in line and "--no-project" not in line:
            return True
    return "uv sync" in workflow_text


def _offenders() -> list[str]:
    """(workflow, script, packages) for every step that cannot import what it imports."""
    out: list[str] = []
    for workflow in sorted(_WORKFLOWS.glob("*.y*ml")):
        text = workflow.read_text(encoding="utf-8")
        parsed = yaml.safe_load(text) or {}
        for job in (parsed.get("jobs") or {}).values():
            for step in job.get("steps") or []:
                run = step.get("run")
                if not run:
                    continue
                for ref in _SCRIPT_REF_RE.findall(run):
                    script = _REPO / ref
                    if not script.is_file():
                        continue
                    packages = _imports_wheel_package(script)
                    if packages and not _provides_project(run, text):
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
    assert not _provides_project("uv run --no-project --with pyyaml python scripts/x.py", "")
    assert _provides_project("uv run python scripts/x.py", "")
    assert _provides_project("python scripts/x.py", "steps:\n  - run: uv sync --frozen\n")
