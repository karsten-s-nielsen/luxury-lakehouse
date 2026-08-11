"""Every ``scripts/`` Databricks client comes from the one shared constructor.

Enforces ADR-075: a cross-cutting concern gets ONE construction site.

``ingestion.databricks_auth.workspace_client()`` installs ``CachedGitHubOidcStrategy`` under
GitHub OIDC, returns a stock client otherwise, and — since 2026-08-10 — converts the SDK's
ambiguous-profile ``ValueError`` into a message that says what to do and that **nothing ran**.

WHY A GATE
----------
The ambiguity fires at CLIENT CONSTRUCTION, so a script that builds its client late fails
*after* the operator believes work is under way. Applying the PR-2a bronze migration hit exactly
that: a pure auth failure read as a partial write. A bare ``WorkspaceClient()`` anywhere in
``scripts/`` reintroduces it.

DETECTION IS AST, NOT REGEX
---------------------------
An earlier pass classified compliance with a regex over the file text and produced **nine false
positives**: files with a *parameter* or *local* named ``workspace_client``
(``bootstrap_artifact_hashes``, ``train_football2vec``, ``train_psxg_hf``), files whose
``--profile`` was a subprocess argument to the ``databricks`` CLI (``create_indexes``), and
files wrapping a bare client in a private ``_workspace_client()`` helper (``trigger_dbt_job``,
``post_dbt_failure_comment``, ``run_dbt_in_databricks``). The property is *"constructs a
zero-argument WorkspaceClient"*, which only the AST can answer.

Constructions passing explicit ``host=``/``token=``/``profile=`` are fine: profile ambiguity
cannot arise when the profile is not being resolved.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO_ROOT / "scripts"
_WORKFLOWS = _REPO_ROOT / ".github" / "workflows"

# The only two zero-arg constructions permitted, each for a stated reason. Pinned as a SET so
# adding OR removing one is a visible, reviewed change — a count would allow a silent swap.
_ALLOWED_BARE: dict[str, str] = {
    "scripts/ci/run_dbt_in_databricks.py": (
        "Runs AS a Databricks job task under ambient runtime auth, where no profile is resolved "
        "and the OIDC strategy is inert — the same exemption workspace_client()'s own docstring "
        "records for refresh_synced_tables."
    ),
    "scripts/migrations/_runner.py": (
        "Carries its own --profile flag and try/except around construction (PR-2a), which is "
        "where this error class was first diagnosed. The bare call is the documented else-branch."
    ),
    "scripts/patch_job_retries.py": (
        "terraform-apply.yml runs this with a bare `python` and a pip-installed databricks-sdk, "
        "WITHOUT the project wheel on sys.path — so importing the helper is a "
        "ModuleNotFoundError, which is exactly how it broke main on 2026-08-11. It runs only "
        "under DATABRICKS_AUTH_TYPE=github-oidc, where no ~/.databrickscfg exists and the "
        "profile ambiguity workspace_client() explains cannot arise. The script's own docstring "
        "carried this reason all along; the ADR-075 sweep edited past it."
    ),
}


def _zero_arg_constructions() -> dict[str, list[int]]:
    """{relative path: [line numbers]} of ``WorkspaceClient()`` with no arguments."""
    out: dict[str, list[int]] = {}
    for path in sorted(_SCRIPTS.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        hits = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "WorkspaceClient"
            and not node.args
            and not node.keywords
        ]
        if hits:
            out[path.relative_to(_REPO_ROOT).as_posix()] = hits
    return out


def _ci_reachable_scripts() -> set[str]:
    """Script filenames referenced by any workflow.

    ``*.y*ml`` rather than ``*.yml``: all ten workflows are ``.yml`` today, but a future
    ``.yaml`` would silently defeat the "a new CI script cannot reintroduce a bare client" claim
    this module makes.
    """
    blob = "\n".join(f.read_text(encoding="utf-8", errors="replace") for f in _WORKFLOWS.glob("*.y*ml"))
    return {f.name for f in _SCRIPTS.rglob("*.py") if "__pycache__" not in f.parts and f.name in blob}


def test_no_unapproved_bare_workspace_client() -> None:
    """Every zero-arg construction is either fixed or explicitly allowed with a reason."""
    offenders = {p: lines for p, lines in _zero_arg_constructions().items() if p not in _ALLOWED_BARE}
    assert not offenders, (
        f"bare WorkspaceClient() in {offenders}. Use "
        "`from ingestion.databricks_auth import workspace_client` — it handles OIDC caching and "
        "turns the ambiguous-profile error into an actionable one."
    )


def test_every_allowance_is_real_and_reasoned() -> None:
    """Both directions: a stale allowance is as bad as a missing one.

    If a file is fixed but left in the allowlist, the allowlist silently widens for whatever is
    written there next.
    """
    actual = set(_zero_arg_constructions())
    for path, reason in _ALLOWED_BARE.items():
        assert reason.strip(), f"{path}: allowance without a reason"
        assert path in actual, f"{path}: allowed but has no bare construction — remove the entry"


def test_ci_reachable_scripts_use_the_shared_helper() -> None:
    """CI scripts run under GitHub OIDC, where the cached strategy is the point.

    Derived from the workflows directory rather than a hard-coded list, so a NEW CI script must
    satisfy this rather than be quietly missed.
    """
    offenders = []
    for name in sorted(_ci_reachable_scripts()):
        path = next(p for p in _SCRIPTS.rglob(name) if "__pycache__" not in p.parts)
        rel = path.relative_to(_REPO_ROOT).as_posix()
        if rel in _ALLOWED_BARE:
            continue
        text = path.read_text(encoding="utf-8")
        if "workspace_client(" in text and not _imports_helper(path):
            offenders.append(rel)
    assert not offenders, f"CI-reachable scripts using workspace_client without importing it: {offenders}"


def _imports_helper(path: pathlib.Path) -> bool:
    """AST check, not a string match.

    ``run_lakebase_grants.py`` imports it combined —
    ``from ingestion.databricks_auth import auth_headers, has_databricks_auth, workspace_client``
    — which an exact-line comparison misses. A gate whose construction check is AST and whose
    import check is a substring is inconsistent in exactly the way that produced this module's
    nine earlier false positives.
    """
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom) and node.module == "ingestion.databricks_auth":
            if any(alias.name == "workspace_client" for alias in node.names):
                return True
    return False


def test_the_gate_is_not_vacuous() -> None:
    """A detector that finds nothing passes while asserting nothing.

    Parse a synthetic module and confirm a zero-arg construction IS caught while an
    explicit-kwargs one is NOT — the distinction the whole gate rests on.
    """
    tree = ast.parse("a = WorkspaceClient()\nb = WorkspaceClient(host='h', token='t')\n")
    hits = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "WorkspaceClient"
        and not n.args
        and not n.keywords
    ]
    assert hits == [1], f"detector is wrong: {hits}"
    assert _ci_reachable_scripts(), "no CI-reachable scripts found — the workflow glob is broken"


@pytest.mark.parametrize("path", sorted(_ALLOWED_BARE))
def test_allowed_files_exist(path: str) -> None:
    assert (_REPO_ROOT / path).is_file(), f"{path} no longer exists; drop the allowance"
