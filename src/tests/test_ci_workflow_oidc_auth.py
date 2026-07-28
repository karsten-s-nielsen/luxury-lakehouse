"""GitHub-Actions OIDC auth-method conflict guard.

Catches the exact class of defect shipped by the #449 PAT->OIDC migration and
observed failing on *every* ``lakebase-grants`` run for a day (2026-07-21/22):

A job sets ``DATABRICKS_CLIENT_ID`` at **job level** (visible to every step)
*and* an OIDC mint step exports ``DATABRICKS_TOKEN`` into ``$GITHUB_ENV``
(visible to every subsequent step). A downstream step that constructs a bare
``WorkspaceClient()`` then sees two configured auth methods -- a bearer token
(pat) and a client id (oauth) -- and the Databricks SDK refuses:

    ValueError: validate: more than one authorization method configured:
                oauth and pat

The first casualty was ``scripts/fix_event_log_ownership.py`` (bare
``WorkspaceClient()``); the earlier heal step only *looked* green because it
carried ``continue-on-error: true``.

The invariant this test enforces:

    A job that (a) sets DATABRICKS_CLIENT_ID at job level AND (b) mints
    DATABRICKS_TOKEN into $GITHUB_ENV must ALSO pin DATABRICKS_AUTH_TYPE at
    job level (any value -- 'pat' or 'github-oidc' -- disambiguates).

Workflows that instead confine ``DATABRICKS_CLIENT_ID`` to the mint step
(python-ci, data-quality-ci) never expose the pair to a downstream bare
client, so they are correctly *not* flagged.

Offline, deterministic, PR-gating -- no live workspace required.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_REPO = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO / ".github" / "workflows"

# --- Materialisation guard (ADR-071 amendment, 2026-07-27) -------------------
# A `DATABRICKS_TOKEN:` assignment whose value interpolates anything (`${{ ... }}`)
# -- a step output, a secret, an expression.
_TOKEN_ASSIGN_RE = re.compile(r"^\s*DATABRICKS_TOKEN\s*:\s*(?P<value>\S.*?)\s*$")
# A shell line exporting DATABRICKS_TOKEN into the env/output files.
_TOKEN_EXPORT_RE = re.compile(r"DATABRICKS_TOKEN\s*=.*GITHUB_(?:ENV|OUTPUT)")
# Name-agnostic sibling. The two rules above both anchor on the literal name
# DATABRICKS_TOKEN, on the theory that a bearer must land there to be usable. That was
# WRONG, and dbt-live-ci.yml proved it: it minted into `token=$TOKEN >> $GITHUB_OUTPUT`
# and passed the value as `--token`, never touching the env var -- so it sailed through a
# rule written to be exhaustive. Any key whose name contains "token" counts now, whatever
# it is called. ACTIONS_ID_TOKEN_REQUEST_* is excluded: that is GitHub's own OIDC request
# plumbing, an INPUT to minting rather than a minted Databricks bearer.
#
# Matches "token" anywhere on the line, not just as the key: `x=$TOKEN` materialises a
# bearer just as surely as `token=$X`, and an anchored key pattern also missed
# MY_BEARER_TOKEN (the `_` before TOKEN is a word character, so `\btoken` never matched).
# Over-flagging is the safe direction for a credential rule -- a false positive costs one
# comment, a false negative cost three months of a dead bearer in a step output.
_ANY_BEARER_EXPORT_RE = re.compile(
    r"^(?!.*ACTIONS_ID_TOKEN_REQUEST).*token.*GITHUB_(?:ENV|OUTPUT)",
    re.IGNORECASE,
)


def _iter_jobs() -> list[tuple[str, str, dict[str, Any]]]:
    """Yield ``(workflow_filename, job_name, job_dict)`` for every job in every workflow."""
    jobs: list[tuple[str, str, dict[str, Any]]] = []
    for wf in sorted(_WORKFLOWS_DIR.glob("*.yml")):
        doc = yaml.safe_load(wf.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        for job_name, job in (doc.get("jobs") or {}).items():
            if isinstance(job, dict):
                jobs.append((wf.name, job_name, job))
    return jobs


def _job_env(job: dict[str, Any]) -> dict[str, Any]:
    env = job.get("env") or {}
    return env if isinstance(env, dict) else {}


def _mints_token_globally(job: dict[str, Any]) -> bool:
    """True if any step writes ``DATABRICKS_TOKEN`` into ``$GITHUB_ENV`` (global export)."""
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        run = step.get("run")
        if isinstance(run, str) and "DATABRICKS_TOKEN=" in run and "GITHUB_ENV" in run:
            return True
    return False


def _violates(job: dict[str, Any]) -> bool:
    """The bug: job-level CLIENT_ID + a global TOKEN mint, with no job-level AUTH_TYPE."""
    env = _job_env(job)
    has_job_client_id = "DATABRICKS_CLIENT_ID" in env
    has_job_auth_type = "DATABRICKS_AUTH_TYPE" in env
    return has_job_client_id and _mints_token_globally(job) and not has_job_auth_type


def test_no_oidc_auth_method_conflict() -> None:
    """No workflow job may expose both a bearer token and a client id to a bare client."""
    offenders = [f"{wf}:{job_name}" for wf, job_name, job in _iter_jobs() if _violates(job)]
    assert not offenders, (
        "OIDC auth-method conflict: these jobs set DATABRICKS_CLIENT_ID at job level "
        "AND mint DATABRICKS_TOKEN into $GITHUB_ENV, but do not pin a job-level "
        "DATABRICKS_AUTH_TYPE. A downstream bare WorkspaceClient() will raise "
        "'more than one authorization method configured: oauth and pat'. Fix: add "
        "'DATABRICKS_AUTH_TYPE: pat' to the job env (the mint step overrides it with a "
        f"step-level github-oidc). Offenders: {offenders}"
    )


def test_detector_flags_the_pre_fix_pattern() -> None:
    """Self-test: the detector must flag the #449 bug and clear both valid fixes.

    Without this, a refactor that quietly breaks ``_violates`` would let the guard
    pass vacuously on a healthy tree while no longer catching a reintroduced bug.
    """
    mint_step = {"run": 'echo "DATABRICKS_TOKEN=$TOKEN" >> "$GITHUB_ENV"'}

    buggy = {
        "env": {"DATABRICKS_HOST": "x", "DATABRICKS_CLIENT_ID": "x"},
        "steps": [mint_step],
    }
    assert _violates(buggy), "detector failed to flag the #449 job-level-CLIENT_ID + global-mint bug"

    fixed_option_a = {  # pin auth_type at job level (this PR's fix)
        "env": {"DATABRICKS_HOST": "x", "DATABRICKS_CLIENT_ID": "x", "DATABRICKS_AUTH_TYPE": "pat"},
        "steps": [mint_step],
    }
    assert not _violates(fixed_option_a), "auth_type=pat at job level must clear the guard"

    fixed_option_b = {  # confine CLIENT_ID to the mint step (python-ci pattern)
        "env": {"DATABRICKS_HOST": "x"},
        "steps": [{"env": {"DATABRICKS_CLIENT_ID": "x"}, **mint_step}],
    }
    assert not _violates(fixed_option_b), "CLIENT_ID confined to the mint step must clear the guard"


def test_guard_actually_scans_workflows() -> None:
    """Fail loudly if the workflow glob ever resolves to nothing (moved/renamed dir)."""
    jobs = _iter_jobs()
    assert jobs, f"no workflow jobs discovered under {_WORKFLOWS_DIR} - guard would pass vacuously"


# ---------------------------------------------------------------------------
# Materialisation guard (ADR-071 amendment, 2026-07-27)
# ---------------------------------------------------------------------------


def _materialisation_offenders(directory: Path | None = None) -> list[str]:
    """Lines that materialise a live Databricks bearer into CI state.

    Two rules, both anchored on ``DATABRICKS_TOKEN`` because that name is the
    CHOKE POINT: a bearer moved by any transport -- ``$GITHUB_ENV``,
    ``$GITHUB_OUTPUT`` + ``steps.*.outputs.*``, ``secrets.*`` -- must eventually
    be assigned to it to be usable. Guarding the assignment therefore guards
    every transport, which guarding one transport does not (the first draft of
    this rule banned ``$GITHUB_ENV`` only and missed ``terraform-apply.yml``,
    which routes the same bearer through a step output).

    A hardcoded literal is allowed: ``dbt_project/profiles.yml`` calls
    ``env_var('DATABRICKS_TOKEN')`` with no default (``:10``, ``:19``, ``:28``,
    ``:53``), so ``dbt deps`` needs a non-empty placeholder to render at parse
    time. Those never authenticate anything.
    """
    offenders: list[str] = []
    for wf in sorted((directory or _WORKFLOWS_DIR).glob("*.yml")):
        for lineno, raw in enumerate(wf.read_text(encoding="utf-8").splitlines(), start=1):
            if raw.lstrip().startswith("#"):
                continue
            if _TOKEN_EXPORT_RE.search(raw) or _ANY_BEARER_EXPORT_RE.search(raw):
                offenders.append(f"{wf.name}:{lineno} exports a bearer into $GITHUB_ENV/$GITHUB_OUTPUT")
                continue
            m = _TOKEN_ASSIGN_RE.match(raw)
            if m and "${{" in m.group("value"):
                offenders.append(f"{wf.name}:{lineno} assigns DATABRICKS_TOKEN from an expression")
    return offenders


def test_no_workflow_materialises_a_databricks_token() -> None:
    """No workflow may turn the self-refreshing OIDC credential into a dead string.

    Root cause of the 2026-07-22..27 scheduled-CI outage. The SDK re-mints on
    EVERY request (``_base_client.py:84`` -> ``:105-110`` ->
    ``credentials_provider.py:494-497`` -> ``oidc_token_supplier.py:16-32``), so a
    reused ``WorkspaceClient(auth_type="github-oidc")`` cannot go stale. Snapshotting
    ``config.authenticate()`` into an env var throws that away and yields a bearer
    that expired mid-job (measured: valid at mint+3:59, ``403 Invalid Token`` at +5:13).
    """
    offenders = _materialisation_offenders()
    assert not offenders, (
        "these workflows materialise a Databricks bearer into CI state. Set "
        "DATABRICKS_AUTH_TYPE=github-oidc at job level and let each WorkspaceClient() "
        "hold the live credential; only a hardcoded placeholder (for dbt parse) may be "
        f"assigned to DATABRICKS_TOKEN. Offenders: {offenders}"
    )


def test_materialisation_detector_flags_both_transports() -> None:
    """Self-test: the detector must catch BOTH transports and clear the placeholder.

    Without this, a refactor that narrows the rule back to ``$GITHUB_ENV`` would pass
    on a clean tree while silently re-admitting the step-output transport.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        wf = Path(tmp) / "probe.yml"
        wf.write_text(
            'a:\n  run: echo "DATABRICKS_TOKEN=$T" >> "$GITHUB_ENV"\n'
            "b:\n  env:\n    DATABRICKS_TOKEN: ${{ steps.mint.outputs.token }}\n"
            "c:\n  env:\n    DATABRICKS_TOKEN: parse-placeholder\n"
            "#  env:\n#    DATABRICKS_TOKEN: ${{ secrets.X }}\n",
            encoding="utf-8",
        )
        found = _materialisation_offenders(Path(tmp))

    assert any("probe.yml:2" in f for f in found), "missed the $GITHUB_ENV transport"
    assert any("probe.yml:5" in f for f in found), "missed the step-output transport"
    assert not any("probe.yml:8" in f for f in found), "flagged an allowed hardcoded placeholder"
    assert len(found) == 2, f"expected exactly the two transports, got {found}"


def test_materialisation_detector_is_name_agnostic() -> None:
    """The rule must not depend on the bearer being called ``DATABRICKS_TOKEN``.

    Regression for the dbt-live-ci miss: ``echo "token=$TOKEN" >> "$GITHUB_OUTPUT"``
    materialised a live bearer and passed the original rule untouched.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        wf = Path(tmp) / "probe.yml"
        wf.write_text(
            'a:\n  run: echo "token=$TOKEN" >> "$GITHUB_OUTPUT"\n'
            'b:\n  run: echo "MY_BEARER_TOKEN=$X" >> "$GITHUB_ENV"\n'
            'c:\n  run: echo "ACTIONS_ID_TOKEN_REQUEST_URL=$U" >> "$GITHUB_ENV"\n'
            'd:\n  run: echo "select_arg=${SELECT_ARG}" >> "$GITHUB_OUTPUT"\n',
            encoding="utf-8",
        )
        found = _materialisation_offenders(Path(tmp))

    assert any("probe.yml:2" in f for f in found), "missed the lowercase `token=` export"
    assert any("probe.yml:4" in f for f in found), "missed a differently-named bearer"
    assert not any("probe.yml:6" in f for f in found), "flagged GitHub's own OIDC request plumbing"
    assert not any("probe.yml:8" in f for f in found), "flagged a non-credential step output"


def test_dbt_live_ci_deploys_the_shim_before_triggering() -> None:
    """The Databricks job runs a workspace COPY of the shim, so CI must redeploy it.

    Without this step the copy is whatever an operator last uploaded by hand. It drifted
    from 2026-04-23 to 2026-07-28 and silently ran a retired code path -- the repo file
    said one thing, the job did another, and nothing failed until dbt's manifest reader
    rejected a manifest written by a different dbt.

    Order matters: uploading AFTER the trigger would deploy for the following run.
    """
    wf = (_WORKFLOWS_DIR / "dbt-live-ci.yml").read_text(encoding="utf-8")
    assert "scripts/upload_ci_shim.py" in wf, (
        "dbt-live-ci must run scripts/upload_ci_shim.py; otherwise edits to "
        "scripts/ci/run_dbt_in_databricks.py never reach the Databricks job."
    )
    assert wf.index("scripts/upload_ci_shim.py") < wf.index("scripts/trigger_dbt_job.py"), (
        "the shim upload must precede the trigger, or the job runs the previous shim."
    )


# ---------------------------------------------------------------------------
# The consumer half of the same invariant
# ---------------------------------------------------------------------------
#
# The rule above stops a WORKFLOW from materialising the bearer. On its own that is only
# half an invariant, and the missing half cost a red main on 2026-07-28: the skip guards
# were migrated to has_databricks_auth() (which correctly reports auth-available under
# OIDC, so the live tests RUN) while seven test bodies still read the raw env var to build
# their connection. Result: `1 passed, 119 errors`, every one a KeyError on a name the
# workflows had — correctly — stopped setting.
#
# Producer and consumer must therefore be guarded together. Guarding either alone leaves a
# tree that is internally inconsistent and only fails once it reaches CI.

_TOKEN_READ_RE = re.compile(r"""os\.environ(?:\.get\(|\[)\s*["']DATABRICKS_TOKEN["']""")

_TEST_ROOTS = ("tests", "src/tests")


def _token_read_offenders(roots: tuple[Path, ...] | None = None) -> list[str]:
    """Test code that reads ``DATABRICKS_TOKEN`` out of the environment directly.

    Such a read is only correct when something materialises the token — which is exactly
    what the producer-side rule forbids. The supported accessor is
    ``ingestion.databricks_auth.bearer_token()`` (raw token, e.g. for
    ``databricks.sql.connect(access_token=...)``) or ``auth_headers()`` (for raw
    ``requests``); both resolve through the SDK, so they work under a static token locally
    AND under OIDC in CI.

    ``has_databricks_auth()`` is deliberately NOT flagged: it reads the variable through a
    helper to decide whether to skip, and returning False on a fork PR is the point.
    """
    search = roots or tuple((_REPO / r) for r in _TEST_ROOTS)
    offenders: list[str] = []
    for root in search:
        if not root.is_dir():
            continue
        for py in sorted(root.rglob("*.py")):
            for lineno, raw in enumerate(py.read_text(encoding="utf-8").splitlines(), start=1):
                if raw.lstrip().startswith("#"):
                    continue
                if _TOKEN_READ_RE.search(raw):
                    # relative_to() only when the file really is under the repo -- the
                    # self-test below scans a tmpdir, which would otherwise raise here.
                    try:
                        label = py.relative_to(_REPO).as_posix()
                    except ValueError:
                        label = py.name
                    offenders.append(f"{label}:{lineno}")
    return offenders


def test_no_test_reads_databricks_token_from_the_environment() -> None:
    """Live tests must obtain credentials through the SDK, not a materialised env var."""
    offenders = _token_read_offenders()
    assert not offenders, (
        "these tests read DATABRICKS_TOKEN directly, so they break the moment CI stops "
        "materialising it (2026-07-28: 119 collection errors on main). Use "
        "ingestion.databricks_auth.bearer_token() for a raw token or auth_headers() for a "
        f"request header. Offenders: {offenders}"
    )


def test_token_read_detector_flags_both_accessor_forms() -> None:
    """Self-test: subscript AND ``.get()`` both count, and the skip helper does not."""
    import tempfile

    # Assembled, never spelled: a literal here would make this file its own offender, and
    # exempting the file by name would blind the rule to a real read added to it later.
    var = "DATABRICKS_" + "TOKEN"

    with tempfile.TemporaryDirectory() as tmp:
        probe = Path(tmp) / "probe.py"
        probe.write_text(
            f'a = os.environ["{var}"]\n'
            f'b = os.environ.get("{var}", "")\n'
            "c = has_databricks_auth()\n"
            f'd = os.environ["{var}"]  # a trailing comment must not exempt the line\n',
            encoding="utf-8",
        )
        found = _token_read_offenders((Path(tmp),))

    assert len(found) == 3, f"expected the two accessor forms plus the commented-suffix line, got {found}"
    assert not any(line.endswith(":3") for line in found), "flagged the skip-guard helper"
