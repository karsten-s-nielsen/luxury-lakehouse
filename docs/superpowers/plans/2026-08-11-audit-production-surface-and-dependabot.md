# Audit the production surface, and resolve Dependabot — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `scripts/audit_resolutions.py` trustworthy, point it at the artifact production actually runs, fix the one live production CVE, and resolve five Dependabot PRs.

**Architecture:** Three units in a fixed order. Unit 1 repairs four defects in the audit script so its verdicts mean something. Unit 2 adds the deployed HuggingFace Space `requirements.txt` as an audited target — the only production surface, currently observed by nothing — fixes `flask` via a `[tool.uv] constraint-dependencies` floor, and teaches `check_cve_blockers.py` to probe production-scope entries. Unit 3 resolves five Dependabot PRs one at a time, with the ADR-046 env-pin lockstep each one needs.

**Tech Stack:** Python 3.10, `uv` 0.9.28, `pip-audit` 2.10.1, `huggingface_hub`, pytest, ruff, pyright.

**Spec:** [`docs/superpowers/specs/2026-08-11-audit-convergence-and-dependabot-design.md`](../specs/2026-08-11-audit-convergence-and-dependabot-design.md) (rev 4). Read it first — it carries the evidence behind every decision below.

---

## Global Constraints

- **Ordering is load-bearing.** Unit 1 → Unit 2 → **Task 5a (deploy seam) → Task 5b (production deploy)** → Unit 3. Unit 3 changes `uv.lock`; doing that while the audit gate is broken means changing the thing the gate watches with the gate disabled. And Unit 3's acceptance check ("all targets CLEAN") is unreachable until Task 5b ships the flask fix to the Space.
- **Observe every new gate RED first.** A gate first seen green is a gate never seen working. Each task below names the exact RED state to observe.
- **Never commit, push, open a PR, or merge without separate explicit operator approval.** Approval of this plan does not grant commit authority.
- **This plan does not define commit boundaries, and must never contain one.** Commits are
  minimal — one per unit of work, decided by the operator at the time, never one per task. Rev 1–9
  of this document printed *"Commit — REQUIRES OPERATOR APPROVAL"* at the end of every task; those
  were never approved commits, they were a template habit, and reading them as boundaries is what
  produced a separate commit for Task 1 and a second queued for Task 2 (folded back into one on
  2026-08-11). A task ends at its verification step. What to commit, and when, is asked — not
  planned. Removed at rev 10.
- **No wheel bump unless a packaged module changes.** The wheel ships only `src/{ingestion,analytics,shared,workflows,evolve}` plus `dbt_project/**` force-includes. `scripts/` and `src/tests/` are NOT packaged. Verify with `uv run python scripts/bump_wheel.py --check`.
- **`hf_taipy_app/requirements.txt` is gitignored and generated at deploy time. Never commit it.** Committing it creates unmanageable false-positive dependabot-pip PRs — a documented decision in `.gitignore`.
- **Gate scripts redirect, never pipe:** `cmd > "$OUT" 2>&1; echo "EXIT=$?" >> "$OUT"`. A pipe reports the exit of the last stage, which is how a failure reads as success.
- **Live-Databricks tests need a fresh OAuth token.** `export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"`. A token older than ~1 hour produces failures indistinguishable from real regressions. If `test_generate_match_key_macro.py`, `test_action_context_live_ddl_parity.py`, `test_orphan_pg_role_absent.py` or `test_sk3_mig_b_orchestrator_invariants.py` fail, re-mint before investigating.
- **Never `yaml.safe_load` + `safe_dump` a human-maintained YAML.** It destroys comments and folded blocks with no test noticing. Splice text.
- **Line length 120.** Ruff `E,W,F,I,N,UP,B,S,BLE,RUF` clean. `pyright` basic, zero errors. **`N802` forbids capitals in test function names.**

**Full local gate suite** (run before declaring any unit done):

```bash
export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"
export SILLY_KICKS_ASSERT_INVARIANTS=1
export PYTHONIOENCODING=utf-8
OUT=/tmp/gates.txt; : > "$OUT"
uv run ruff check src/ scripts/ >> "$OUT" 2>&1; echo "RUFF=$?" >> "$OUT"
uv run ruff format --check src/ scripts/ >> "$OUT" 2>&1; echo "FMT=$?" >> "$OUT"
uv run lint-imports >> "$OUT" 2>&1; echo "IMP=$?" >> "$OUT"
# NOT `pyright src/` — narrower than CI, so it passes on code the pipeline rejects.
uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py >> "$OUT" 2>&1; echo "PYRIGHT=$?" >> "$OUT"
uv run python scripts/bump_wheel.py --check >> "$OUT" 2>&1; echo "BUMP=$?" >> "$OUT"
uv run python scripts/pip_audit_ignores.py --check >> "$OUT" 2>&1; echo "IGNORES=$?" >> "$OUT"
uv run pytest src/tests/ -q >> "$OUT" 2>&1; echo "PYTEST=$?" >> "$OUT"
grep -E "=[0-9]+$" "$OUT"
```

---

## Open Decisions — BLOCKING, operator-owned

Two questions this plan must not answer for itself. Both change what the cycle covers.
**Do not start Unit 2 until both are resolved; record the answers here before implementing.**

### Open Decision A — RESOLVED 2026-08-11: **A1, the production deploy is in scope**

Implemented as **Task 5b**, between Unit 2 and Unit 3. Task 9 Step 5's "all targets CLEAN" is
achievable only after Task 5b completes. The rejected options are kept below for the record.

<details><summary>Original framing and options</summary>

#### Does the production deploy enter this cycle?

Task 5's constraint fixes the *next* deploy. The Space keeps running `flask 3.1.1` until someone
runs `manage_space.py deploy production`. So as drafted, merging Unit 2 leaves
`cve-blocker-review.yml` **failing every Monday** until that deploy happens — and Unit 3's per-PR
acceptance check ("all targets CLEAN", Task 9 Step 5) has no achievable pass state.

That is the harm D10 argues against — a standing false alarm that trains readers to ignore the
gate — arriving through the front door.

| Option | Blast radius |
|---|---|
| **A1 — bring the deploy into the cycle** as an operator step between Unit 2 and Unit 3, with the Space target going CLEAN as its acceptance check | Adds a production deploy to the cycle. Keeps Task 4's RED demonstration. Cycle is not "done" until production actually runs the fix — which is arguably when it *is* done |
| **A2 — land Task 5 + deploy BEFORE adding the Space target (Task 4)** | No red window at all. Costs the RED demonstration: the target is added against already-fixed production and is first seen green, which this plan's own Global Constraints forbid |
| **A3 — time-boxed ignore entry for `PYSEC-2026-2151`, removed at deploy** | No deploy needed this cycle. Creates an exception to hide a *known-fixed* finding, in the cycle whose `Follows:` is ADR-075 |

**Recommendation: A1.** The constraint is not shipped until the Space runs it; treating the deploy
as outside the cycle is what creates the contradiction. A1 is also the only option that keeps both
the RED demonstration and a green gate. A3 I would not take — it is precisely the pattern
ADR-075 closed.

**Whichever is chosen, Task 9 Step 5's "all targets CLEAN" must be restated to match.**

</details>

### Open Decision B — RESOLVED 2026-08-11: **B3, no dev target**

**One gate per surface, no shared ownership:**

| surface | owner | cadence |
|---|---|---|
| dev tooling | `python-ci.yml:181`, installed env (216 pkgs on linux) | every PR |
| production resolutions (base / taipy-app / dbt / sdk) | this job, `--no-default-groups` | weekly |
| deployed artifact (both Spaces) | this job, fetched from HF | weekly |

So each advisory has exactly one place it can be reported, and the two gates cannot disagree or
drift apart.

Measured 2026-08-11 with environment markers evaluated for the runner's platform (linux / CPython
3.10 / x86_64) and names PEP 503-normalised — python-ci's installed environment is **216** packages,
and every fork's dev-side sits inside it:

| resolution | prod | dev-side | dev-side NOT covered by python-ci |
|---|---|---|---|
| base | 54 | 128 | **0** |
| taipy-app | 140 | 97 | **0** |
| dbt | 115 | 110 | **0** |
| sdk | 61 | 127 | **0** |

A weekly dev target would therefore re-audit, less often, a set already covered per PR. What
python-ci structurally cannot see is `taipy-app` prod (48 packages) and `dbt` prod (35):
`dbt-core`, `dbt-databricks`, `apispec`, `boto3`, `automat`, `agate`.

`base` and `sdk` prod are also 0-uncovered today but stay as targets — two cheap runs that catch
the day someone adds a base dependency python-ci's extras happen not to pull.

**216, not 218 — and why the direction of that error matters.** A raw `uv export` carries every
platform's pins, so using one as a proxy for an *install* inflates the covering set, biasing toward
the "0 uncovered" conclusion B3 rests on. Re-measured with markers evaluated, the conclusion holds;
the two excluded pins are `pywin32==311` and `waitress==3.0.2`, both `sys_platform == 'win32'`.

Those two are audited by neither gate, and **B3 is not the cause**. They reach python-ci only via
the `analytics`/`embeddings`/`mlflow`/`jax` extras, and appear in **none** of this job's four
resolutions — verified: 0 hits in `base --no-default-groups`, in `base` *with* the dev group, and in
`taipy-app` prod. The weekly job never saw them before this cycle and does not lose them now. The
residual gap is that no gate audits win32-only pins on a linux runner, which is pre-existing,
out of scope here, and harmless in practice: neither package can execute in the Space's linux
container or on the runner.

Implemented in **Task 2**. There was no prior decision to preserve: the weekly job's dev coverage
was never chosen, it is `uv export` including groups by default, which *is* spec defect 3.

### Open Decision C — RESOLVED 2026-08-11: **C2, add the seam**

`_compile_requirements()` runs inside the upload path (`manage_space.py:467` and `:521`), so
`deploy staging` and `deploy production` each resolve independently — staging validated one
artifact and production shipped another.

Implemented as **Task 5a**: a `--no-compile` / `--expect-sha256` seam routed through a single
`_prepare_requirements()`, so one compiled pin set is validated and then shipped to both Spaces.
`--no-compile` without `--expect-sha256` is rejected by the parser, so the safe path is the only
path. Task 5b Step 4's staging≡production assert (the C1 detective check) is **kept** as end-to-end
confirmation that the seam behaves on live artifacts.

**ADR: written.** `docs/superpowers/adrs/ADR-076-deploy-the-validated-requirements-artifact.md`,
at `Status: Proposed`; Task 5a Step 6 flips it to `Accepted` and commits it with the code. It
records the rejected alternatives a future maintainer will otherwise re-litigate — in particular
why the file is not simply committed to the repo (a second pin set with no sync tool and no CI
drift check — **not** the Dependabot reason `.gitignore:133-137` still gives, which expired at #450)
and why production is not pinned to `uv.lock` (the Space would inherit `mlflow`'s
`cryptography <50` ceiling for a package it never installs, losing the fixed 50.0.0 it gets today).

---

## File Structure

| File | Responsibility | Unit |
|---|---|---|
| `scripts/audit_resolutions.py` | Modify — the four defects, plus the deployed-Space target | 1, 2 |
| `src/tests/test_audit_resolutions.py` | Modify — classification + target tests | 1, 2 |
| `scripts/check_cve_blockers.py` | Modify — `Outcome` enum (Task 1, rev 9a); `scope:` dispatch (Task 6) | 1, 2 |
| `scripts/check_cve_blockers.py` | Modify — `scope:` dispatch to a production prober | 2 |
| `src/tests/test_check_cve_blockers.py` | Modify — production-probe tests | 2 |
| `pyproject.toml` | Modify — `flask>=3.1.3` constraint | 2 |
| `.pip-audit-ignores.yml` | Modify — `scope:` field documented in the header | 2 |
| `src/tests/test_pip_audit_ignores.py` | Modify — `scope:` schema assertions | 2 |
| `.github/workflows/cve-blocker-review.yml` | Modify — add `--with huggingface-hub` (Task 4a); confirm-only in Unit 1 | 2 |
| `scripts/manage_space.py` | Modify — the `--no-compile` / `--expect-sha256` seam (Task 5a) | 2 |
| `src/tests/test_manage_space_requirements.py` | Create — seam tests + the one-call-site guard | 2 |
| `docs/superpowers/adrs/ADR-076-deploy-the-validated-requirements-artifact.md` | **Already written** — commit it with Task 5a | 2 |
| `.gitignore` | Modify — retire the stale dependabot-pip rationale (Task 5a Step 6) | 2 |
| `docs/superpowers/adrs/ADR-075-…md` | Modify — record the production-surface decision | 2 |

---

## Baseline facts (measured 2026-08-11 — do not re-derive, but DO re-verify if anything looks off)

```
uv export --extra taipy-app                     237 packages
uv export --extra taipy-app --no-dev            141 packages   (96-package dev group)
uv pip compile … --extra taipy-app              136 packages   (production)
version differences lock vs production           51            (50 backwards, 1 forwards)
```

Production runs `flask==3.1.1` (advisory `PYSEC-2026-2151`, fixed in 3.1.3), `cryptography==50.0.0`
(the *fixed* version — `mlflow` is not in this extra), `taipy-gui==4.1.2`, `taipy-rest==4.1.1`.

Space repo ids per `scripts/manage_space.py`: production `luxury-lakehouse/soccer-analytics-app`
(156 files), staging `luxury-lakehouse/staging` (158 files). Both public, both contain
`requirements.txt`.

---

# UNIT 1 — make the audit trustworthy

## Task 1: Structured classification with an UNKNOWN outcome

`audit()` returns a raw exit code and `main()` maps any non-zero to "findings", so a failure to
*run* is reported as *vulnerabilities found*. That is how a `FileNotFoundError` printed
`FAIL: unignored findings in 4 resolution(s)`.

**Files:**
- Modify: `scripts/audit_resolutions.py:128-152` (`audit`), `:164-200` (`main`)
- Test: `src/tests/test_audit_resolutions.py`

**Interfaces** (revised at rev 9 — see Revision history; the shapes below are what was built):
- Produces: `Outcome(str, Enum)` with members `CLEAN` / `FINDINGS` / `UNKNOWN`, `__str__`
  overridden to return the value. A frozen `AuditResult(outcome, detail, diagnostics="")`.
  `classify_audit(returncode: int, stdout: str, stderr: str = "") -> AuditResult` — the single
  construction site for a verdict. `bound_diagnostics(text, *, head, tail) -> str` and
  `report_diagnostics(name: str, result: AuditResult) -> None`.
- Consumes: nothing from earlier tasks.

**Why not `tuple[str, str]` and three bare string constants** (the rev-8 shape): the tuple was at
capacity, and the third fact it could not carry is the one that matters — `audit()` discarded
pip-audit's stderr, so the UNKNOWN this task exists to produce would have printed
`pip-audit did not produce a JSON report (exit 1)` and **nothing else**, telling the reader to fix
the runner while withholding the only evidence for doing so. Discarding at the subprocess boundary
is irreversible; declining to print is a policy the caller can change. Bare string constants also
make a typo'd `== "CLEAR"` silently False rather than a type error. The result object mirrors
`check_cve_blockers.Result` — parallel shape, deliberately **not** a shared type, since
"did this resolution audit clean" and "did this floor resolve" are different domains.

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_audit_resolutions.py`:

```python
class TestAuditClassification:
    """A failure to RUN must never be reported as vulnerabilities FOUND.

    On 2026-08-11 a FileNotFoundError in the project build made the job print
    `FAIL: unignored findings in 4 resolution(s): base, taipy-app, dbt, sdk` — four fabricated
    CVE regressions. This is the same BLOCKED/UNKNOWN rule check_cve_blockers.py already applies.
    """

    def test_clean_audit_is_clean(self) -> None:
        out = '{"dependencies": [{"name": "flask", "version": "3.1.3", "vulns": []}], "fixes": []}'
        assert classify_audit(0, out)[0] == CLEAN

    def test_findings_are_findings(self) -> None:
        out = (
            '{"dependencies": [{"name": "flask", "version": "3.1.1", '
            '"vulns": [{"id": "PYSEC-2026-2151"}]}], "fixes": []}'
        )
        assert classify_audit(1, out)[0] == FINDINGS

    def test_output_that_is_not_json_is_unknown(self) -> None:
        """The audit did not run. Reporting this as FINDINGS cries wolf; as CLEAN it certifies
        a claim nothing tested."""
        outcome, detail = classify_audit(1, "FileNotFoundError: Forced include not found: …")
        assert outcome == UNKNOWN
        assert "did not produce a JSON report" in detail

    def test_zero_exit_with_unparseable_output_is_also_unknown(self) -> None:
        """Exit code alone decides nothing — the report is the evidence."""
        assert classify_audit(0, "")[0] == UNKNOWN

    def test_no_findings_but_nonzero_exit_is_unknown(self) -> None:
        """Self-contradictory: the shape a partial --strict collection failure takes. Reading it
        as CLEAN certifies a set pip-audit is saying it could not fully assess."""
        out = '{"dependencies": [{"name": "flask", "version": "3.1.3", "vulns": []}], "fixes": []}'
        assert classify_audit(1, out)[0] == UNKNOWN
```

Assertions read `classify_audit(...).outcome is Outcome.CLEAN` (identity, not `==`). Add to the
imports at the top of that file:

```python
from scripts.audit_resolutions import Outcome, bound_diagnostics, classify_audit
```

Three further tests belong in the same commit, because each is the executable form of a rule that
is otherwise only a comment:

- `test_stderr_never_changes_the_verdict` — the verdict must be invariant to stderr content across
  every branch. `classify_audit` accepts stderr **only** so the result can carry it; matching it
  would make a security gate depend on an upstream tool's wording, which is not an API (spec D2).
  The one stderr signal that must count — a dependency that could not be collected — already
  arrives structurally, as a non-zero exit under `--strict`.
- `test_unparseable_stdout_is_evidence_too` — when stdout is not a report it is the other half of
  what the tool said; keeping only stderr discards it. On a *parseable* run stdout is the report
  and must NOT be folded into diagnostics, or it buries them.
- `test_both_ends_survive_and_the_elision_is_announced` — a size bound is itself something that can
  hide the answer. Keep head and tail (a uv resolver error leads with its summary, a traceback ends
  with the exception type) and state the omitted count. The same rule caps the named-package list
  in a FINDINGS detail with `and N more`.

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run pytest src/tests/test_audit_resolutions.py -q
```
Expected: `ImportError: cannot import name 'Outcome'`.

- [ ] **Step 3: Implement**

In `scripts/audit_resolutions.py`, beside the existing constants (near `_AUDIT_TIMEOUT_S`):

```python
class Outcome(str, Enum):
    """`str` mixin because 3.10 has no StrEnum and the summary formats these into a fixed-width
    column; `__str__` overridden so `str(x)` and `f"{x}"` agree — without it 3.10 gives
    `Outcome.CLEAN` for the first and `CLEAN` for the second."""

    CLEAN = "CLEAN"
    FINDINGS = "FINDINGS"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class AuditResult:
    """The verdict AND the evidence behind it. `diagnostics` is never an input to `outcome`."""

    outcome: Outcome
    detail: str
    diagnostics: str = ""


def classify_audit(returncode: int, stdout: str, stderr: str = "") -> AuditResult:
    ...
```

Branch order is unchanged from rev 8 — unparseable → UNKNOWN (carrying stdout **and** stderr as
evidence, since unparseable stdout is not a report), non-list `dependencies` → UNKNOWN, any
`vulns` → FINDINGS, non-zero exit with a clean report → UNKNOWN, else CLEAN. Two helpers sit
beside it: `_collect_diagnostics(stderr, *, unparsed_stdout="")` and the public
`bound_diagnostics(text, *, head=20, tail=20)`.

Add `json`, `textwrap`, `dataclasses.dataclass` and `enum.Enum` to the stdlib imports.

- [ ] **Step 4: Run the test — it passes**

```bash
uv run pytest src/tests/test_audit_resolutions.py -q
```
Expected: PASS.

- [ ] **Step 5: Wire it into `audit()` and `main()`**

Replace `audit()`'s body (`:128-152`) so it returns the classification, and add `-f json`:

```python
def audit(requirements_path: Path) -> AuditResult:
    """Run pip-audit over a requirements file and classify the result.

    `--no-project` so uv does NOT build the editable install: pip-audit audits a requirements
    FILE and never needed the project. Building it hits the hatchling force-include of the
    gitignored dbt_project/dbt_packages and fails on any clean checkout.

    `--strict` so a dependency that could not be collected fails instead of passing as clean.
    `pip-audit` is PINNED — an unpinned security tool changes behaviour under the gate silently.
    """
    cmd = [
        "uv", "run", "--no-project", "--with", f"pip-audit=={_PIP_AUDIT_VERSION}",
        "pip-audit", "-r", str(requirements_path),
        "--no-deps", "--strict", "-f", "json",
        *flags(load_ignores()),
    ]
    result = subprocess.run(  # noqa: S603 — argv from constants plus generated ignore flags
        cmd, cwd=_REPO, capture_output=True, text=True, check=False, timeout=_AUDIT_TIMEOUT_S
    )
    return classify_audit(result.returncode, result.stdout, result.stderr)
```

> **AMENDED 2026-08-12 — the argv above is INCOMPLETE as shipped; do not re-implement it verbatim.**
> `--no-deps` does not stop pip-audit resolving. pip-audit 2.10.1 gates its venv-free path on
> `--disable-pip` alone (`_dependency_source/requirement.py:161`) and `--no-deps` only makes that
> flag *legal to pass*, so this argv built a throwaway venv and installed each locked resolution to
> rediscover versions the file already pinned. On the runner that venv's `ensurepip` exits 1 —
> every target of `cve-blocker-review.yml` classified UNKNOWN from the day it was created, and the
> job was never once green. The shipped `cmd` therefore carries **`"--disable-pip"`** between
> `"--no-deps"` and `"--strict"`. Measured: `sdk` 36s → 1s; all four resolutions 10s locally and
> **8.1s on CI**, where the job then went green for the first time (run `31592589093`: 54/140/115/61
> packages CLEAN). Same dependency set, same findings, 0 skipped. See ADR-075's amended Consequences.

**Pass `result.stderr`.** Dropping it here is irreversible and is what left the original
`FileNotFoundError` invisible behind a one-line UNKNOWN.

Add beside the constants:

```python
#: Pinned deliberately. pip-audit's JSON shape and --strict semantics are the gate's contract;
#: a silent upgrade could change either. Bump this in a reviewed commit, never implicitly.
_PIP_AUDIT_VERSION = "2.10.1"
```

Update `audit_resolution()` (`:154-162`) to return `tuple[AuditResult, list[str]]` and add the
printer that closes the diagnostic gap:

```python
def report_diagnostics(name: str, result: AuditResult) -> None:
    """Print the captured tool output for a verdict that is not CLEAN.

    Only when it is not CLEAN: pip-audit writes progress to stderr, and 163 clean packages of it
    is how a log stops being read.
    """
    if not result.diagnostics:
        return
    print(f"  --- {name}: captured tool output ---", file=sys.stderr)
    print(textwrap.indent(bound_diagnostics(result.diagnostics), "  "), file=sys.stderr)
```

`main()` branches on the outcome and prints the evidence beside it:

```python
            if result.outcome is not Outcome.CLEAN:
                report_diagnostics(name, result)
                failures.append((name, result))
```

and the summary:

```python
    if failures:
        outcomes = {result.outcome for _, result in failures}
        print(f"FAIL: {len(failures)} resolution(s) not clean:", file=sys.stderr)
        for name, result in failures:
            print(f"  {result.outcome:8s} {name}: {result.detail}", file=sys.stderr)
        if Outcome.FINDINGS in outcomes:
            print(
                "\nFINDINGS: add each advisory to .pip-audit-ignores.yml with a blocked_by "
                "re-derived by EXECUTION (scripts/check_cve_blockers.py), or take the fix.",
                file=sys.stderr,
            )
        if Outcome.UNKNOWN in outcomes:
            print(
                "\nUNKNOWN means the audit did not run — this is an infrastructure failure, "
                "NOT a set of CVE regressions. Fix the runner before reading anything into it. "
                "The captured tool output is printed above each UNKNOWN.",
                file=sys.stderr,
            )
        return 1
```

**Keep the FINDINGS guidance.** The pre-existing summary carried "add each advisory … with a
`blocked_by` re-derived by EXECUTION, or take the fix"; that is a standing fence and rev 8's
replacement block silently dropped it. It now fires only when a FINDINGS is present, so the two
messages cannot both shout at a reader facing one of them.

- [ ] **Step 6: Verify end-to-end and commit**

```bash
uv run ruff format scripts/audit_resolutions.py src/tests/test_audit_resolutions.py
uv run ruff check scripts/ src/ && uv run pytest src/tests/test_audit_resolutions.py -q
PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_resolutions.py --only base
```
Expected: the `base` resolution audits and prints `CLEAN`, with no `dbt_packages` error. Measured
2026-08-11: `CLEAN: 163 package(s) audited, no unignored advisories` (163 because Task 2 has not
yet dropped the dev group; it becomes 55).

**Then execute the failing path.** A CLEAN run exercises none of the diagnostic machinery, and
this cycle has already shipped two guards that could not fail. Force a real subprocess failure and
confirm the cause reaches the log:

```bash
PYTHONPATH=. PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe -c "
import tempfile, pathlib
from scripts.audit_resolutions import audit, report_diagnostics
d = pathlib.Path(tempfile.mkdtemp())
p = d / 'requirements.txt'
p.write_text('this-package-does-not-exist-xyzzy==1.2.3\n', encoding='utf-8')
r = audit(p)
print('OUTCOME=' + str(r.outcome))
report_diagnostics('forced-failure', r)
"
```

Expected, measured 2026-08-11: `OUTCOME=UNKNOWN` **and** a `captured tool output` block naming
`Could not find a version that satisfies the requirement this-package-does-not-exist-xyzzy==1.2.3`.
An UNKNOWN with an empty block means the diagnostics are not being carried and the guard is
decorative.

---

## Task 2: Exclude the dev group from every target

`uv export` includes default dependency groups, so this job reports dev-tooling advisories as
production exposure. Measured: 237 exported for `--extra taipy-app` vs 141 with
`--no-default-groups` — a 96-package delta of `torch`, the `nvidia-*-cu12` CUDA stack, `pytest`,
`ruff`, `pyright`, `scikit-learn`, `openevolve` and `pip-audit` itself, none of which the Space
contains.

**No dev target replaces it** (Open Decision B → B3). `python-ci.yml:181` already audits the
installed environment — `uv sync --extra analytics --extra embeddings --extra mlflow --extra jax`
plus the dev group, **216 packages** on linux — on every PR, through the same ignore list. Measured
2026-08-11 with markers evaluated for the runner's platform, every fork's dev-side sits entirely
inside that set:

| resolution | dev-side packages | not covered by python-ci |
|---|---|---|
| base | 128 | **0** |
| taipy-app | 97 | **0** |
| dbt | 110 | **0** |
| sdk | 127 | **0** |

Measure this with markers evaluated, never from a raw `uv export`: an export carries every
platform's pins, so it inflates the covering set in exactly the direction that would manufacture a
"0 uncovered" result. See Open Decision B for the two win32-only pins this excludes and why they
are not a regression.

A weekly dev target would re-audit, less often, a set already covered per-PR — and give one
advisory two owners. What python-ci structurally *cannot* see is **taipy-app prod (48 packages)**
and **dbt prod (35)** — `dbt-core`, `dbt-databricks`, `apispec`, `boto3`, `automat`, `agate`. That
is this job's surface, and after this task it is the whole of it.

**Files:**
- Modify: `scripts/audit_resolutions.py:105-126` (`export_resolution`)
- Test: `src/tests/test_audit_resolutions.py`

`RESOLUTIONS` (`:73`) and `label()` (`:84`) are **unchanged** — B3 needs one flag, not a new type.

**Interfaces:**
- Consumes: `classify_audit`, `CLEAN`, `FINDINGS`, `UNKNOWN` from Task 1.
- Produces: `_export_cmd(extra: str | None) -> list[str]`. `export_resolution(extra: str | None) -> str`
  keeps its existing signature.

- [ ] **Step 1: Write the failing test**

```python
class TestDevGroupExclusion:
    """Dev tooling is python-ci.yml's surface, not this job's.

    Measured 2026-08-11: `uv export --extra taipy-app` yields 237 packages, with
    --no-default-groups 141. The 96-package delta is the dev group. Auditing it here made torch
    and setuptools advisories read as production exposure when the Space contains neither.
    """

    def test_every_export_excludes_default_groups(self) -> None:
        for extra in RESOLUTIONS:
            assert "--no-default-groups" in _export_cmd(extra), f"{label(extra)} would include dev"

    def test_the_flag_is_unconditional(self) -> None:
        """Anti-drift, asserted on BEHAVIOUR and SIGNATURE — never by scraping the source.

        The flag must not depend on the argument (so exhaustive inputs, including an invented
        extra), and no second parameter may exist for it to depend on (so a future
        `_export_cmd(extra, include_dev=False)` cannot reopen the defect while the loop below
        still passes).
        """
        for extra in (None, "taipy-app", "dbt", "sdk", "not-a-real-extra"):
            assert "--no-default-groups" in _export_cmd(extra), f"omitted for {extra!r}"
        params = inspect.signature(_export_cmd).parameters
        assert list(params) == ["extra"], f"unexpected parameters: {list(params)}"

    def test_no_dev_target_is_defined(self) -> None:
        """B3: python-ci.yml audits the installed env on EVERY PR — 216 packages with markers
        evaluated for linux — and all four forks' dev-side packages are inside it, 0 uncovered,
        measured 2026-08-11. A dev target here adds no coverage and gives one advisory two
        owners."""
        assert not [r for r in RESOLUTIONS if r and "dev" in r]

```

**Do NOT add a `test_every_conflicting_extra_is_audited` here.** Rev 8 specified one, written
without reading the module it was being appended to — `TestResolutionCoverage` has carried that
assertion since the previous cycle. Rev 8's version was nonetheless the *better* one (it compared
declared extras against the raw `RESOLUTIONS`, which is what `--extra` receives, rather than
against `label()`, which is display-only and identity by coincidence), so **Task 1's commit merged
its comparison basis into the existing test** and kept the existing failure message. One
assertion, in the class about coverage. This class is about the export FLAG — a different
property.

Import `_export_cmd`, `RESOLUTIONS`; add `import inspect` if absent.

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run pytest src/tests/test_audit_resolutions.py::TestDevGroupExclusion -q
```
Expected: `ImportError: cannot import name '_export_cmd'`.

- [ ] **Step 3: Implement**

```python
def _export_cmd(extra: str | None) -> list[str]:
    """Build the `uv export` argv for one resolution.

    `--no-default-groups` is UNCONDITIONAL. uv export includes dependency groups by default, and
    this project's dev group is ~127 packages of test/lint/ML tooling that no deployed artifact
    contains. Dev tooling is audited by python-ci.yml against the installed environment on every
    PR — a superset of every fork's dev-side, measured 2026-08-11 — so re-auditing it weekly here
    would add no coverage while giving one advisory two owners.

    Split from execution so the flags are unit-testable without spawning uv.
    """
    cmd = [
        "uv", "export", "--no-hashes", "--no-emit-project",
        "--format", "requirements-txt", "--no-default-groups",
    ]
    if extra is not None:
        cmd += ["--extra", extra]
    return cmd
```

and in `export_resolution`, replace the hand-built command with `cmd = _export_cmd(extra)`, leaving
the rest of the function as it stands.

- [ ] **Step 4: Run the tests**

```bash
uv run pytest src/tests/test_audit_resolutions.py -q
```
Expected: PASS.

- [ ] **Step 5: Verify the exclusion is real**

```bash
uv run python -c "
from scripts.audit_resolutions import RESOLUTIONS, export_resolution, label
for r in RESOLUTIONS:
    lines = [l for l in export_resolution(r).splitlines() if '==' in l]
    dev = [l for l in lines if l.startswith(('pytest==','ruff==','torch=='))]
    print(f'{label(r):12s} packages={len(lines):4d}  dev-markers={len(dev)}')
"
```

Expected, measured 2026-08-11 against `main` @ `45393362`:

| target | packages | dev-markers |
|---|---|---|
| `base` | **55** | 0 |
| `taipy-app` | **141** | 0 |
| `dbt` | **116** | 0 |
| `sdk` | **62** | 0 |

**141, not 237.** 237 is `--extra taipy-app` *with* the dev group — the pre-fix number, and the one
the spec quotes when describing the defect. Seeing 141, and zero dev-markers on every row, is the
fix working.

- [ ] **Step 6: Confirm the workflow invocation**

`.github/workflows/cve-blocker-review.yml:113` runs
`uv run --no-project --with pyyaml python scripts/audit_resolutions.py`. For Unit 1 this needs no
change — the script now spawns its own pinned pip-audit as a subprocess.

**Unit 2 changes this. See Task 4a — do not carry this "no change needed" forward.**

---

## Task 3: Observe the whole of Unit 1 green in CI

- [ ] **Step 1: Run the full gate suite** (Global Constraints block above). Expected: all zero.
- [ ] **Step 2: After the operator merges, dispatch the workflow**

```bash
BEFORE=$(gh run list --workflow=cve-blocker-review.yml --limit 1 --json databaseId -q '.[0].databaseId')
gh workflow run cve-blocker-review.yml --ref main
# Poll until a NEW run id appears — `--limit 1` immediately after dispatch returns the PREVIOUS
# run, and reading its (green) conclusion is how a broken gate gets signed off.
until [ "$(gh run list --workflow=cve-blocker-review.yml --limit 1 --json databaseId -q '.[0].databaseId')" != "$BEFORE" ]; do sleep 5; done
RUN=$(gh run list --workflow=cve-blocker-review.yml --limit 1 --json databaseId -q '.[0].databaseId')
gh run watch "$RUN" --exit-status
gh run view "$RUN" --json jobs -q '.jobs[] | "\(.name): \(.conclusion)"'
```

- [ ] **Step 3: Confirm BOTH jobs pass.** `check-blockers` and `audit-resolutions`. The previous
      dispatch (run `31505206091`) had `check-blockers` succeed and `audit-resolutions` fail; a
      green `audit-resolutions` on a clean runner is Unit 1's definition of done.

---

# UNIT 2 — audit the deployed artifact, and fix flask

## Task 4: Add the deployed Space requirements as an audited target

The production surface is observed by nothing: no dependabot ecosystem covers `hf_taipy_app/`, its
requirements file is gitignored *specifically* to hide it from dependabot-pip, `uv.lock` is a
different resolution, and the surface mutates without a commit. Auditing a fresh
`uv pip compile` at audit time would be a third resolution at a third moment — matching neither the
last deploy nor the next. Fetch what is deployed.

**Files:**
- Modify: `scripts/audit_resolutions.py`
- Test: `src/tests/test_audit_resolutions.py`

**Interfaces:**
- Consumes: `audit` (returns `AuditResult`), `Outcome`, `report_diagnostics`,
  `strip_local_versions` from Tasks 1–2.
- Produces: `SPACE_REPOS: tuple[tuple[str, str], ...]` = `(("space-production", "luxury-lakehouse/soccer-analytics-app"), ("space-staging", "luxury-lakehouse/staging"))`
  and `fetch_space_requirements(repo_id: str) -> str`.

- [ ] **Step 1: Write the failing test**

```python
class TestDeployedSpaceTarget:
    """Audit what is DEPLOYED, not a fresh resolve.

    hf_taipy_app/requirements.txt is generated at deploy time by `uv pip compile`, which always
    takes the newest satisfying release — so two deploys of the same commit can ship different
    versions. Re-compiling at audit time would certify a pin set that may never have existed in
    production. manage_space.py uploads the folder via upload_folder and requirements.txt is not
    in IGNORE_PATTERNS, so the real pin set is one download away.
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
```

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run pytest src/tests/test_audit_resolutions.py::TestDeployedSpaceTarget -q
```
Expected: `ImportError: cannot import name 'SPACE_REPOS'`.

- [ ] **Step 3: Implement**

```python
#: The deployed Spaces. Ids duplicated from manage_space.py and asserted equal by test, because
#: an audit pointed at a stale Space id passes forever while watching nothing.
SPACE_REPOS: tuple[tuple[str, str], ...] = (
    ("space-production", "luxury-lakehouse/soccer-analytics-app"),
    ("space-staging", "luxury-lakehouse/staging"),
)


def fetch_space_requirements(repo_id: str) -> str:
    """Download the requirements.txt a Space is actually running.

    No token. Both Spaces are public, and huggingface_hub picks up a cached CLI login on its own
    if one exists.

    Deliberately NOT ingestion.utils.resolve_hf_token(): it is declared `-> str` and returns the
    EMPTY STRING when nothing is found (utils.py:805-816), which reaches hf_hub_download as
    token="" and builds a bare `Bearer ` header — the exact httpx.LocalProtocolError footgun
    CLAUDE.md's Orchestration Discipline documents. Passing nothing lets the library decide.
    It also cannot be imported here: this script runs under `uv run --no-project`, so the wheel
    is not on sys.path on a clean runner.
    """
    from huggingface_hub import hf_hub_download

    path = hf_hub_download(repo_id=repo_id, repo_type="space", filename="requirements.txt")
    return Path(path).read_text(encoding="utf-8")
```

`main()`'s `--only` filter currently matches against `RESOLUTIONS` only (`scripts/audit_resolutions.py:167-175`). Extend it to cover the Space names too, or
`--only space-production` — the RED demonstration in Step 4 — exits `unknown resolution`:

```python
    known = [label(r) for r in RESOLUTIONS] + [n for n, _ in SPACE_REPOS]
    parser.add_argument("--only", help=f"audit a single target ({', '.join(known)})")
    ...
    if args.only and args.only not in known:
        print(f"ERROR: unknown target {args.only!r}", file=sys.stderr)
        return 1
```

then gate each loop with `if args.only and args.only != name: continue`.

**Keep the unknown-name rejection** (it exists today at `:173-175`). A bare in-loop `continue`
without it turns a typo'd `--only` into "audited nothing, exited 0" — a gate that passes by
auditing an empty set is worse than one that errors.

In `main()`, after the `RESOLUTIONS` loop, audit each Space with the same `audit()` and
classification. A fetch failure is **UNKNOWN**, not FINDINGS:

```python
        for name, repo_id in SPACE_REPOS:
            print(f"\n=== auditing deployed artifact: {name} ({repo_id}) ===", file=sys.stderr)
            try:
                text = fetch_space_requirements(repo_id)
            except Exception as exc:  # noqa: BLE001 — a fetch failure proves nothing either way
                # The traceback IS the evidence here: "no module named huggingface_hub" and "the
                # Space was deleted" are the same one-line UNKNOWN without it (see Task 4a).
                failed = AuditResult(
                    Outcome.UNKNOWN,
                    f"could not fetch requirements.txt: {exc}",
                    traceback.format_exc(),
                )
                print(f"  {failed.outcome}: {failed.detail}", file=sys.stderr)
                report_diagnostics(name, failed)
                failures.append((name, failed))
                continue
            rewritten, subs = strip_local_versions(text)
            path = Path(tmp) / f"requirements-{name}.txt"
            path.write_text(rewritten, encoding="utf-8")
            for note in subs:
                print(f"  local-version proxy: {note}", file=sys.stderr)
            result = audit(path)
            print(f"  {result.outcome}: {result.detail}", file=sys.stderr)
            if result.outcome is not Outcome.CLEAN:
                report_diagnostics(name, result)
                failures.append((name, result))
```

`AuditResult` is constructed directly on the fetch-failure path — the only place outside
`classify_audit` that does so, because there is no pip-audit run to classify. Use
`traceback.format_exc()`, not `str(exc)`: `ModuleNotFoundError: No module named 'huggingface_hub'`
(Task 4a's defect) and a deleted Space produce the same one-line UNKNOWN otherwise. Add
`import traceback`.

- [ ] **Step 4: Run the tests, then observe the target RED**

```bash
uv run pytest src/tests/test_audit_resolutions.py -q
PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_resolutions.py --only space-production
```
Expected: **FINDINGS — `flask 3.1.1` (`PYSEC-2026-2151`)**. This is the RED state. Do not proceed to
Task 5 until you have seen it. If it comes back CLEAN, the target is not reaching the deployed file.

---

## Task 4a: Give the workflow `huggingface_hub`, and prove it in CI

**No local check in this plan can catch this defect.** Measured 2026-08-11:

```
$ uv run --no-project --with pyyaml python -c "import sys; print(sys.prefix)"
prefix: D:\Development\karstenskyt__luxury-lakehouse\.venv
$ uv run --no-project --with pyyaml python -c "import huggingface_hub as h; print(h.__file__)"
  …\.venv\lib\site-packages\huggingface_hub\__init__.py
```

`--no-project` skips *installing the project*; it does **not** stop uv layering onto a discovered
`.venv`. So `import huggingface_hub` succeeds on any dev machine and fails on the runner
(`uv python install 3.10`, no `.venv`).

Worse, it fails **silently by design**: the import sits inside `fetch_space_requirements`, whose
caller maps every exception to UNKNOWN. The gate would fail every Monday with
`could not fetch requirements.txt: No module named 'huggingface_hub'` — reading as infrastructure
noise, not a missing dependency.

This is defect 1's failure mode exactly — *"it passed locally because the dev machine already had
it."* Unit 2 must not reproduce, in a new place, the bug Unit 1 exists to fix.

**Files:**
- Modify: `.github/workflows/cve-blocker-review.yml:113`

- [ ] **Step 1: Add the dependency**

```yaml
        run: >-
          uv run --no-project
          --with pyyaml
          --with huggingface-hub==1.6.0
          python scripts/audit_resolutions.py
```

Add this rationale as a YAML comment above the step, because the version looks wrong at a glance:

```yaml
      # huggingface-hub is pinned to the LOCK's version (1.6.0), not the newest release (1.27.0).
      # The gate must run what the project resolves, and an unpinned dependency in a security
      # gate can change behaviour between two runs with no commit in between. This pin does NOT
      # ride the ADR-046 lockstep — re-check it whenever uv.lock moves.
```

`1.6.0` is what `uv.lock` carries, verified 2026-08-11. Re-check before implementing
(`grep -A2 '^name = "huggingface-hub"' uv.lock`) — Unit 3 bumps the lock.

- [ ] **Step 2: Reproduce the runner's isolation locally**

```bash
cd "$(mktemp -d)" && git clone --depth 1 file://D:/Development/karstenskyt__luxury-lakehouse repo
cd repo
# RED first — this is the defect, and a fresh clone is the only place in this plan it can appear:
uv run --no-project --with pyyaml python -c "import huggingface_hub"
# Expected: ModuleNotFoundError, and sys.prefix is an ephemeral uv build dir, NOT a .venv.
# GREEN — with the SAME pin the workflow ships. Not bare `--with huggingface-hub`: that installs
# latest (1.27.0) and would verify a version the gate never runs.
uv run --no-project --with pyyaml --with huggingface-hub==1.6.0 python -c "import huggingface_hub, yaml; print('ok')"
```

Verified 2026-08-11 that both the `file://D:/…` and `file:///D:/…` spellings clone cleanly under
Git Bash — no MSYS path mangling here.

- [ ] **Step 3: Prove it in CI — this is Unit 2's own dispatch**

After the operator merges Unit 2, repeat Task 3's dispatch-and-poll block. Unit 2 needs its own CI
observation because Task 3's covers Unit 1 only, and every other Unit 2 verification runs against a
`.venv` that cannot reproduce the runner.

Expected: `audit-resolutions` reaches the Space targets and reports a real outcome for each — **not**
`could not fetch requirements.txt`. Read the job log and confirm the phrase
`auditing deployed artifact:` appears twice.

---

## Task 5: Fix flask with a production constraint

Not an ignore entry — the fix is available. Measured: constraining `flask>=3.1.3` on the production
compile gives **136 packages before, 136 after; nothing added, nothing removed**, and four versions
move: `flask 3.1.1→3.1.3`, `taipy-rest 4.1.1→4.1.0`, `apispec 6.8.2→6.6.1`,
`apispec-webframeworks 1.2.0→1.1.0`. `taipy-rest` and `apispec` appear nowhere in `hf_taipy_app/` —
they arrive only as transitives of the `taipy` metapackage.

**Files:**
- Modify: `pyproject.toml` (the `[tool.uv] constraint-dependencies` block, ~line 414)

- [ ] **Step 1: Re-verify the measurement before changing anything**

```bash
printf 'flask>=3.1.3\n' > /tmp/c_flask.txt
uv pip compile pyproject.toml --extra taipy-app --python-version 3.10 \
    --python-platform linux -c /tmp/c_flask.txt -o /tmp/after.txt
uv pip compile pyproject.toml --extra taipy-app --python-version 3.10 \
    --python-platform linux -o /tmp/before.txt
uv run python -c "
import re
def pins(p):
    return {m.group(1).lower():m.group(2) for m in
            (re.match(r'^([A-Za-z0-9._-]+)==([^\s;]+)', l) for l in open(p)) if m}
a,b = pins('/tmp/before.txt'), pins('/tmp/after.txt')
print('before',len(a),'after',len(b))
print('added',sorted(set(b)-set(a)),'removed',sorted(set(a)-set(b)))
for k in sorted(set(a)&set(b)):
    if a[k]!=b[k]: print(' ',k,a[k],'->',b[k])
"
```
Expected: `before 136 after 136`, `added [] removed []`, and exactly the four moves above. **If the
numbers differ, STOP and re-open the spec — upstream has moved and the decision needs re-taking.**

- [ ] **Step 2: Add the constraint**

In `pyproject.toml`'s `[tool.uv] constraint-dependencies` list, after `"pyasn1>=0.6.4",`:

```toml
    # PYSEC-2026-2151 (flask < 3.1.3). The Space resolves flask 3.1.1 because taipy-rest 4.1.1
    # caps it at <=3.1.1; taipy-rest 4.1.0 does not, so this floor takes 4.1.0 instead. Measured
    # 2026-08-11: 136 packages before and after, nothing added or removed, and only taipy-rest +
    # apispec + apispec-webframeworks move — none of which appear anywhere in hf_taipy_app/.
    # This is the D5 carve-out: a backwards move accepted as the measured, named, bounded price
    # of a security fix.
    "flask>=3.1.3",
```

- [ ] **Step 3: Re-lock and sync the env pins (ADR-046) — expect a NO-OP**

```bash
uv lock
git diff --quiet uv.lock && echo "lock unchanged (expected)" || echo "LOCK CHANGED — STOP"
uv run python scripts/sync_tf_env_pins.py
uv run python scripts/sync_tf_env_pins.py --check
```

`uv.lock` already sits at the constrained versions — measured 2026-08-11 on `main` @ `45393362`:

```
flask 3.1.3    taipy-rest 4.1.0    apispec 6.6.1
```

The lock resolves the whole workspace at once and already prefers this combination. The divergence
is on the production side, where `uv pip compile` resolves fresh and takes the *newest*
`taipy-rest` (4.1.1), which caps flask at 3.1.1. So this constraint changes **nothing in the lock**
and everything at deploy time.

**A non-empty `uv.lock` diff here means the constraint did something unintended — stop and
investigate rather than committing it.**

- [ ] **Step 4: Confirm the target is STILL RED — the constraint alone does not fix production**

```bash
PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_resolutions.py --only space-production
```
Expected: **still FINDINGS.** The Space is still running the requirements.txt compiled at the last
deploy. A constraint in `pyproject.toml` binds the *next* compile, not a file already uploaded.
Task 5b is what closes this. Say so in the commit message so a reader of the history does not
mistake the merge for the fix.

- [ ] **Step 5: Full gate suite.**

---

## Task 5a: Make the deploy ship the artifact that was validated

**Open Decision C → C2.** `_compile_requirements()` runs inside the upload path — at
`manage_space.py:467` for `--dry-run` and again at `:521` immediately before `upload_folder`. So
today, `deploy staging` and `deploy production` each resolve independently and a validated pin set
is never the shipped pin set. This task adds the seam that makes "the artifact you tested is the
artifact you ship" true, and Task 5b then uses it.

**Files:**
- Modify: `scripts/manage_space.py` — `_compile_requirements` (`:156-201`), `_dry_run` (`:465-467`),
  `_deploy` (`:512-521`), `_deploy_command` (`:573-588`), `main` (`:669-709`, extract
  `_build_parser`), the deploy subparser (`:688-698`)
- Create: `src/tests/test_manage_space_requirements.py`

**Interfaces:**
- Produces: `_compile_requirements() -> str` (now returns the sha256 it wrote);
  `_requirements_sha256() -> str`;
  `_prepare_requirements(*, compile_it: bool, expect_sha256: str | None) -> str` — the **single**
  site from which requirements are compiled or verified.
- Consumes: nothing from earlier tasks.

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_manage_space_requirements.py`:

```python
"""The artifact that is validated must be the artifact that ships.

`_compile_requirements()` used to be called from two places inside the upload path, so
`deploy staging` and `deploy production` each ran their own `uv pip compile`. Because a fresh
compile always takes the newest satisfying release, staging validated one pin set and production
shipped another — a staging gate that could not gate. See ADR / Open Decision C.
"""

import ast
import hashlib
from pathlib import Path

import pytest

from scripts import manage_space

_REPO = Path(__file__).resolve().parents[2]


class TestRequirementsSeam:
    def test_compile_is_called_from_exactly_one_place(self) -> None:
        """The structural guarantee. Two call sites IS the defect; this fails if one comes back,
        regardless of how the flags behave.

        Parsed, never grepped. A `source.count("_compile_requirements()")` assertion is broken by
        any docstring that names the function — including the one on _prepare_requirements
        explaining why a single call site exists. The repo has an AST-guard precedent in
        src/tests/_delta_write_ast.py.
        """
        tree = ast.parse((_REPO / "scripts" / "manage_space.py").read_text(encoding="utf-8"))

        def calls_it(node: ast.AST) -> bool:
            return any(
                isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id == "_compile_requirements"
                for n in ast.walk(node)
            )

        sites = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == "_compile_requirements"]
        assert len(sites) == 1, f"expected exactly 1 call site, found {len(sites)}"

        callers = [f.name for f in ast.walk(tree)
                   if isinstance(f, ast.FunctionDef) and f.name != "_compile_requirements"
                   and calls_it(f)]
        assert callers == ["_prepare_requirements"], callers

    def test_verify_mode_rejects_a_mismatched_hash(self, tmp_path, monkeypatch) -> None:
        """Fail-closed. Shipping an unverified file is the failure this seam exists to prevent."""
        req = tmp_path / "requirements.txt"
        req.write_text("flask==3.1.3\n", encoding="utf-8")
        monkeypatch.setattr(manage_space, "_requirements_path", lambda: req)
        with pytest.raises(manage_space.SpaceError, match="sha256 mismatch"):
            manage_space._prepare_requirements(compile_it=False, expect_sha256="0" * 64)

    def test_verify_mode_accepts_the_matching_hash_without_compiling(self, tmp_path, monkeypatch) -> None:
        req = tmp_path / "requirements.txt"
        req.write_text("flask==3.1.3\n", encoding="utf-8")
        digest = hashlib.sha256(req.read_bytes()).hexdigest()
        monkeypatch.setattr(manage_space, "_requirements_path", lambda: req)
        monkeypatch.setattr(
            manage_space, "_compile_requirements",
            lambda: pytest.fail("must not compile when verifying a pinned artifact"),
        )
        assert manage_space._prepare_requirements(compile_it=False, expect_sha256=digest) == digest

    def test_verify_mode_requires_the_file_to_exist(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(manage_space, "_requirements_path", lambda: tmp_path / "absent.txt")
        with pytest.raises(manage_space.SpaceError, match="does not exist"):
            manage_space._prepare_requirements(compile_it=False, expect_sha256="0" * 64)

    def test_no_compile_without_an_expected_hash_is_rejected_by_the_cli(self) -> None:
        """--no-compile alone would ship whatever happens to be on disk — a stale file from an
        earlier session included. Requiring the hash makes the safe path the only path.

        Requires main() to accept argv (Step 4). Today it is `def main() -> int` reading
        sys.argv, so this call would raise TypeError, not SystemExit.
        """
        with pytest.raises(SystemExit):
            manage_space.main(["deploy", "staging", "--no-compile"])

    def test_a_production_deploy_must_ship_a_validated_artifact(self) -> None:
        """Structural, not opt-in. Mirrors _require_force_for_production (:299-303): production
        gets the safety gate, staging keeps the ergonomic path where the artifact is produced."""
        with pytest.raises(SystemExit):
            manage_space.main(["deploy", "production"])

    def test_staging_may_still_compile(self) -> None:
        """The gate must not make the normal staging workflow impossible — that is how a safety
        check gets routed around."""
        import argparse

        parser = manage_space._build_parser()
        args = parser.parse_args(["deploy", "staging"])
        assert isinstance(args, argparse.Namespace) and not args.no_compile
```

- [ ] **Step 2: Run them and watch them fail**

```bash
uv run pytest src/tests/test_manage_space_requirements.py -q
```
Expected: `AttributeError: module 'scripts.manage_space' has no attribute '_prepare_requirements'`.

- [ ] **Step 3: Implement the seam**

```python
def _requirements_path() -> Path:
    """The compiled pin set that `upload_folder` ships. Indirected for tests."""
    return Path(__file__).parent.parent / "hf_taipy_app" / "requirements.txt"


def _requirements_sha256() -> str:
    return hashlib.sha256(_requirements_path().read_bytes()).hexdigest()


def _prepare_requirements(*, compile_it: bool, expect_sha256: str | None) -> str:
    """Produce or verify the pin set, and return its sha256. The ONLY entry point for both.

    Two call sites to `_compile_requirements()` inside the upload path meant `deploy staging` and
    `deploy production` resolved independently, so the artifact validated on staging was never the
    artifact shipped to production. Everything now routes through here, and the returned digest is
    what makes "same artifact" checkable rather than assumed.
    """
    if compile_it:
        return _compile_requirements()

    path = _requirements_path()
    if not path.exists():
        msg = f"--no-compile given but {path} does not exist — nothing to ship"
        raise SpaceError(msg)
    actual = _requirements_sha256()
    if actual != expect_sha256:
        msg = (
            f"requirements sha256 mismatch: expected {expect_sha256}, found {actual}. "
            "The file on disk is not the artifact that was validated — recompile and re-verify."
        )
        raise SpaceError(msg)
    logger.info("Verified requirements artifact sha256=%s (not recompiled)", actual)
    return actual
```

Change `_compile_requirements`'s return annotation to `-> str` and end it with:

```python
    digest = _requirements_sha256()
    logger.info("Compiled requirements: %s (sha256=%s)", _requirements_path(), digest)
    return digest
```

**Route `_compile_requirements`'s own path through `_requirements_path()` too.** It currently
rebuilds it independently at `:167-168` (`repo_root = Path(__file__).parent.parent`,
`relative_output = "hf_taipy_app/requirements.txt"`) and again at `:200`. Leaving that would create
a second construction site for the same path inside the very commit whose ADR cites ADR-075 against
exactly that. Keep the *relative* string for the `-o` argument — that part is the fence described
below — but derive the absolute path from `_requirements_path()`:

```python
    try:
        relative_output = _requirements_path().relative_to(repo_root).as_posix()
    except ValueError:
        # Patched to a tmp_path outside the repo (tests). Keep working rather than raising a
        # ValueError that reads as a path bug instead of a test-setup detail.
        relative_output = str(_requirements_path())
```

The `try` matters: the tests monkeypatch `_requirements_path` to a `tmp_path`, which is **not**
under `repo_root`, so a bare `relative_to` raises `ValueError: '…' is not in the subpath of '…'`.
None of the four tests reach the compile path today, so this is a trap rather than a break — but it
would fire on the first test that patches the path *and* compiles.

This also makes `monkeypatch.setattr(manage_space, "_requirements_path", ...)` govern both the
compile and verify paths, instead of only the verify path.

**Do not touch the relative `-o` argument.** Its docstring (`:163-165`) says the relative path keeps
the autogenerated header comment matching "CI's freshness check". Nothing in `.github/workflows/`
currently greps `requirements.txt`, so that check may have moved or lapsed — either way, changing
the path form is a separate question from this task.

Replace the call at `:467` and `:521` with `_prepare_requirements(compile_it=compile_it,
expect_sha256=expect_sha256)`, threading both parameters through `_dry_run`, `_deploy` and
`_deploy_command` as keyword-only arguments. Add `import hashlib`.

- [ ] **Step 4: Add the CLI flags, fail-closed**

```python
    p_deploy.add_argument(
        "--no-compile",
        action="store_true",
        help="Ship the requirements.txt already on disk instead of recompiling. "
             "Requires --expect-sha256 so a stale file cannot be shipped silently.",
    )
    p_deploy.add_argument(
        "--expect-sha256",
        metavar="HEX",
        help="Assert the on-disk requirements.txt has this sha256 before uploading.",
    )
```

**`main()` must accept argv, and the parser must be reachable without running a deploy.** Today it
is `def main() -> int` at `:669`, building the parser inline and reading `sys.argv` at `:709`. Two
mechanical changes, matching the house pattern in `scripts/audit_resolutions.py:164`:

```python
def _build_parser() -> argparse.ArgumentParser:
    """The CLI surface, separated from execution so flags can be asserted without deploying."""
    parser = argparse.ArgumentParser(...)   # the existing :671-708 body, moved verbatim
    ...
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)          # was: parser.parse_args()

    if getattr(args, "no_compile", False) and not args.expect_sha256:
        # parser is in scope here (:671) — fail-closed before any Space is touched.
        parser.error("--no-compile requires --expect-sha256 (refusing to ship an unverified file)")

    if args.command == "deploy" and args.target == "production" and not args.no_compile:
        parser.error(
            "production deploys must ship an artifact that was validated: "
            "pass --no-compile --expect-sha256 <digest from the pre-deploy manifest>"
        )
```

The second check makes the guarantee **structural where it matters**, rather than opt-in per
deploy. It follows the existing `_require_force_for_production(target, force)` convention
(`manage_space.py:299-303`, used at `:316` and `:641`), which already refuses destructive
operations on production without an explicit flag — same shape, same reasoning, one more
operation. Staging keeps the compiling path, because compiling is the normal thing to do there
and staging is where the artifact is *produced*.

Both checks fire after `parse_args` (`:709`) and before `repo_id = TARGETS[args.target]` and
`api = HfApi()`, so no Space object is constructed on the failing path.

`argv=None` preserves every existing call: `parse_args(None)` reads `sys.argv` exactly as today, so
the `if __name__ == "__main__"` entry point and any console-script registration are unaffected.

- [ ] **Step 5: Run the tests**

```bash
uv run pytest src/tests/test_manage_space_requirements.py -q
```
Expected: PASS.

- [ ] **Step 6: Flip ADR-076 to Accepted**

`docs/superpowers/adrs/ADR-076-deploy-the-validated-requirements-artifact.md` is **already written**
and sits at `Status: Proposed`. Change it to `Accepted` in this commit — an ADR describing a seam
that now exists should not still read as a proposal — and it ships **with** the code, not after.

Also correct `.gitignore`'s comment above `hf_taipy_app/requirements.txt` (around `:133-137`). It
currently attributes the exclusion to Dependabot treating "every requirements.txt pin as direct,
which is unmanageable" — true when written, retired at **#450** when the `pip` ecosystem was
replaced by `uv`. Both this plan and ADR-076 cite that comment as authority, so leaving it stale
propagates the error. Append one clause:

```
# (Historic: the `pip` ecosystem was replaced by `uv` in #450, so this no longer generates PRs.
#  The file stays generated because a committed copy would be a second pin set to keep in step
#  with uv.lock, with no CI check regenerating and diffing it — see ADR-076 alternative B.)
```

Files this task touches: `scripts/manage_space.py`,
`src/tests/test_manage_space_requirements.py`, `.gitignore`, and
`docs/superpowers/adrs/ADR-076-deploy-the-validated-requirements-artifact.md`.

No wheel bump: `scripts/` is not one of the five packaged directories. Confirm with
`uv run python scripts/bump_wheel.py --check`.

---

## Task 5b: Deploy production, and watch the target go green

The security fix is not shipped until the Space runs it. This is the step that makes Unit 3's
acceptance criterion achievable, and it is the acceptance criterion for Unit 2.

**This is an operator action requiring explicit approval** — it changes what users are running.

- [ ] **Step 1: Deploy from a checkout that contains the merged constraint**

`manage_space.py` compiles `hf_taipy_app/requirements.txt` from `pyproject.toml` at deploy time, so
the constraint must be present in the working tree. Deploying from a stale checkout silently ships
the unfixed pin set and the target stays red for a reason that looks like the tool failing.

```bash
git checkout main && git pull --ff-only
git log --oneline -1 && grep -n 'flask>=3.1.3' pyproject.toml   # both must be present
.venv/Scripts/python.exe scripts/manage_space.py deploy staging --dry-run
```

Note `.venv/Scripts/python.exe`, not `uv run` — `uv run python scripts/manage_space.py` deadlocks
at an inner `uv build` (documented; near-zero CPU on a live process is the signature).

- [ ] **Step 2: Run D5's assertion — a full change manifest, not a three-package grep**

D5 requires that *"any change to what the Space installs is gated by an explicit
no-package-moves-backwards assertion"* (spec §3, D5). **Task 5b is the only place in this cycle
where D5 can actually be violated**, and a grep for three package names would not enforce it: a
fresh `uv pip compile` takes the newest satisfying release of *everything*, so it can move any of
the 136 packages without those three changing.

The `--dry-run` writes the requirements it would ship — verified 2026-08-11: `_dry_run` calls
`_compile_requirements()` as its first statement (`scripts/manage_space.py:465-467`), so the file
is regenerated before anything is previewed and without anything being uploaded.

Diff it against what production is running **right now**, reusing Task 4's fetcher:

```bash
export PYTHONIOENCODING=utf-8
PYTHONPATH=. .venv/Scripts/python.exe -c "
import hashlib, re, sys
from pathlib import Path
from packaging.version import Version, InvalidVersion
from scripts.audit_resolutions import fetch_space_requirements
PIN = re.compile(r'^([A-Za-z0-9._-]+)==([^\s;]+)')
def pins(text):
    return {m.group(1).lower(): m.group(2) for m in map(PIN.match, text.splitlines()) if m}
REQ  = Path('hf_taipy_app/requirements.txt')
if not REQ.exists():
    print('FAILED: ' + str(REQ) + ' does not exist -- Step 1 did not run, or ran elsewhere')
    sys.exit(1)
now  = pins(fetch_space_requirements('luxury-lakehouse/soccer-analytics-app'))
nxt  = pins(REQ.read_text(encoding='utf-8'))
# Carve by exact TRANSITION, never by name. A name-keyed set would exempt any move of these three
# -- taipy-rest 4.1.1 -> 1.0.0 would pass and be issued a digest. D5 bounds the flask fix's price;
# it does not licence arbitrary downgrades of the packages that fix happens to touch.
CARVE = {'taipy-rest': ('4.1.1','4.1.0'),
         'apispec': ('6.8.2','6.6.1'),
         'apispec-webframeworks': ('1.2.0','1.1.0')}
added, removed, back = sorted(set(nxt)-set(now)), sorted(set(now)-set(nxt)), []
for name in sorted(set(now) & set(nxt)):
    if now[name] == nxt[name]:
        continue
    try:
        direction = 'BACKWARD' if Version(nxt[name]) < Version(now[name]) else 'forward'
    except InvalidVersion:
        direction = 'UNCOMPARABLE'
    print(f'  {direction:13s} {name}: {now[name]} -> {nxt[name]}')
    if direction != 'forward' and CARVE.get(name) != (now[name], nxt[name]):
        back.append(name)
print(f'added={added}')
print(f'removed={removed}')
# The manifest measures DIRECTIONS of change; on its own it cannot tell a correct no-op from a
# stale artifact that happens to match production. Assert the fix this deploy exists to ship is
# actually present, or an unfixed leftover passes green and the sha256 seam faithfully certifies
# the wrong bytes -- which is the 'deploying from a stale checkout' failure Step 1 names.
if nxt.get('flask') != '3.1.3':
    print('FAILED: flask==3.1.3 absent from the artifact -- the constraint did not reach this compile')
    sys.exit(1)
if back or added or removed:
    print('FAILED: unapproved move(s) -- no digest issued; nothing may be deployed from this run')
    sys.exit(1)
# Emitted ONLY on success, and only for the exact bytes this manifest parsed. Step 3 consumes
# this value, so a regeneration between validation and upload fails the deploy rather than being
# tracked silently. Printing it unconditionally would let an operator copy a digest out of a
# FAILED manifest and deploy an artifact this script just rejected.
print('VALIDATED_SHA256=' + hashlib.sha256(REQ.read_bytes()).hexdigest())
"
```

**ASCII only inside the `-c` payload** (CLAUDE.md → Orchestration Discipline). An em-dash in the
failure message mangles to `?` on the default Windows console and under `cp1252` — corrupting the
text at the exact moment it is read. Exit codes are unaffected, so the gate still fails closed, but
use `--`. Export `PYTHONIOENCODING=utf-8` before this step as the Global Constraints gate suite does.

Expected on success: `flask 3.1.1 -> 3.1.3` forward, the three carve-out transitions BACKWARD,
nothing added or removed, exit 0, one `VALIDATED_SHA256=` line.

**Any other backwards move, any add/remove, a missing file, or a missing flask fix exits 1 — stop
and do not deploy.** That is not a formality: it is the one measurement D5 exists to force, and rev
1 of the spec was corrected precisely for stating D5 and then proposing a change that violated it on
50 packages because direction was never measured.

Four refusal paths, each of which was a hole first (executed 2026-08-11 across six scenarios):

| scenario | result |
|---|---|
| `taipy-rest 4.1.1 -> 1.0.0` (carve-out abuse) | FAILED — transition-keyed carve-out rejects it; a name-keyed set issued a digest |
| stale artifact identical to production | FAILED — the flask assertion rejects it; direction checks alone passed it green |
| `hf_taipy_app/requirements.txt` absent | FAILED with an explanation, not a `FileNotFoundError` traceback |
| `odd-pkg 1.0.0 -> alpha` | FAILED — `UNCOMPARABLE` counts as backward, fail-closed |

In every failing scenario **no digest is printed**, so one cannot be copied out of a rejected run.

**Record the `VALIDATED_SHA256=` value this prints.** Step 3 requires it, and it must come from
here — from the tool that read the bytes — not be recomputed later from the file.

Do not commit `hf_taipy_app/requirements.txt`; it is gitignored. (The `.gitignore` comment
attributes this to dependabot-pip spam, which stopped being true at **#450** when the `pip`
ecosystem was replaced by `uv` — see ADR-076 alternative B. The instruction stands; that reason
does not.)

- [ ] **Step 3: Deploy the VALIDATED artifact — same pin set to both Spaces**

Task 5a's seam is what makes this a real gate — **but only if the expectation comes from Step 2's
manifest.** Paste the `VALIDATED_SHA256=` value it printed:

```bash
SHA=<paste VALIDATED_SHA256 from Step 2>     # NOT `sha256sum hf_taipy_app/requirements.txt`
.venv/Scripts/python.exe scripts/manage_space.py deploy staging --no-compile --expect-sha256 "$SHA"
.venv/Scripts/python.exe scripts/manage_space.py status staging
```
Wait for the staging Space to build and report running before touching production.

```bash
.venv/Scripts/python.exe scripts/manage_space.py deploy production --no-compile --expect-sha256 "$SHA"
.venv/Scripts/python.exe scripts/manage_space.py status production
```

> **Do not re-derive `$SHA` from the file here.** `_prepare_requirements(compile_it=False, …)`
> computes `_requirements_sha256()` of that same file, so `SHA=$(sha256sum …)` would assert
> `sha256(file) == sha256(file)` — a comparison that **cannot fail**, evaluated in the one mode
> that never rewrites the file. The guard would be vacuous, and the property ADR-076 claims
> (*validated artifact == shipped artifact*) would be assumed rather than proven.
>
> Sourcing the digest from the manifest is what makes it evidence: if anything regenerated the
> file between validation and upload — an interrupted retry, a second terminal, a re-run
> `--dry-run` to re-read its output — the deploy fails instead of silently shipping an
> uninspected artifact.

If either deploy raises `sha256 mismatch`, the file changed after it was validated. Re-run Step 2's
manifest against the new file and take its new digest; never override.

> Without Task 5a this sequence would perform **four independent resolves**: the dry-run compiles
> pin set **A** (which Step 2's manifest inspects), `deploy staging` compiles and ships **B**,
> `deploy production` compiles and ships **C** — staging validating B while production shipped C.
> That is §1.2's own finding, a fresh compile always taking the newest satisfying release, applied
> to this task's own gate.
>
> Note staging is **158 files** to production's **156**, so it remains an approximation of
> production in other respects — the pin set is now identical, the file set is not.

- [ ] **Step 4: The acceptance check — targets go GREEN, and the two Spaces agree**

```bash
PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_resolutions.py --only space-production
PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_resolutions.py --only space-staging
```
Expected: **CLEAN**, both. This is the first moment the production surface has ever been both
observed and clean, and it is Unit 2's definition of done.

Then prove staging actually gated production, rather than assuming it:

```bash
PYTHONPATH=. .venv/Scripts/python.exe -c "
from scripts.audit_resolutions import fetch_space_requirements as f
a = f('luxury-lakehouse/staging'); b = f('luxury-lakehouse/soccer-analytics-app')
import re
P = re.compile(r'^([A-Za-z0-9._-]+)==([^\s;]+)')
pa = {m.group(1).lower(): m.group(2) for m in map(P.match, a.splitlines()) if m}
pb = {m.group(1).lower(): m.group(2) for m in map(P.match, b.splitlines()) if m}
diff = {k: (pa.get(k), pb.get(k)) for k in set(pa) | set(pb) if pa.get(k) != pb.get(k)}
print('IDENTICAL' if not diff else f'DIVERGED on {len(diff)}: {diff}')
"
```

Expected: `IDENTICAL` — and after Task 5a this holds **by construction**, not by luck: both deploys
uploaded the same sha256-verified file. Keep the assert anyway. It is the end-to-end confirmation
that the seam did what the unit tests claim, measured on the live artifacts rather than on mocks,
and it is the check that would catch a `--no-compile` flag silently not being honoured.

- [ ] **Step 5: Confirm in CI**

Re-run Task 4a Step 3's dispatch. `audit-resolutions` must now be green end-to-end. Until this
passes, Unit 3 does not start — its per-PR acceptance check assumes a clean baseline.

---

## Task 6: `scope:` dispatches the blocker probe

`check_cve_blockers.py` probes by splicing a floor into `[tool.uv] constraint-dependencies` and
running `uv lock`. A blocker that lives in the production resolution cannot be proven that way. The
field must *select a prober*, never *exempt an entry* — an exempt entry is a hand-verified claim
that ages silently, which is the ADR-075 failure mode this tooling exists to close.

**Files:**
- Modify: `scripts/check_cve_blockers.py`, `.pip-audit-ignores.yml` (header), `src/tests/test_pip_audit_ignores.py`
- Test: `src/tests/test_check_cve_blockers.py`

**Interfaces:**
- Consumes: `BLOCKED`, `COLLATERAL`, `MOVED`, `UNKNOWN`, `classify`, `Result` (existing, `scripts/check_cve_blockers.py:92-95, 162, 277`).
- Produces: `entry_scope(entry: dict[str, str]) -> str` returning `"lock"` or `"production"`;
  `probe_production(entry, *, timeout_s) -> Result`.

- [ ] **Step 1: Write the failing test**

```python
class TestScopeDispatch:
    """`scope:` selects WHICH resolution to probe. It never exempts an entry from probing.

    The lock probe splices a floor into constraint-dependencies and runs `uv lock`. An entry whose
    cap exists only in the production resolution would be probed against a lock that does not have
    the cap — returning MOVED ("take the fix") every week for a fix production cannot take, and
    flipping to BLOCKED after any lock change. Exempting it instead would reintroduce exactly the
    silently-ageing hand-verification ADR-075 closed.
    """

    def test_scope_defaults_to_lock(self) -> None:
        assert entry_scope({"id": "X"}) == "lock"

    def test_production_scope_is_recognised(self) -> None:
        assert entry_scope({"id": "X", "scope": "production"}) == "production"

    def test_unknown_scope_raises_rather_than_defaulting(self) -> None:
        """Silently treating a typo'd scope as `lock` probes the wrong resolution and reports a
        confident wrong answer."""
        with pytest.raises(ProbeError, match="unknown scope"):
            entry_scope({"id": "X", "scope": "prod"})

    def test_production_scope_entries_are_still_checkable(self) -> None:
        """The whole point: no entry escapes probing because of its scope."""
        entry = {"id": "X", "package": "flask", "fix_in": "3.1.3", "scope": "production"}
        assert is_checkable(entry)


class TestProductionProbeOutcomes:
    """An unsatisfiable production floor must read BLOCKED, never UNKNOWN.

    BLOCKED is the verdict the whole mechanism exists to produce. Collapsing it into UNKNOWN
    makes a genuine upstream cap indistinguishable from an offline runner — defect 2's confusion
    class, in the prober built to fix defect 2.
    """

    def test_unsatisfiable_floor_is_blocked_not_unknown(self, monkeypatch) -> None:
        no_solution = (
            "  x No solution found when resolving dependencies:\n"
            "  |-> Because taipy-gui>=4.1.1 depends on flask>=3.1.0,<3.2 and flask>=99.0, "
            "we can conclude that taipy-gui>=4.1.1 cannot be used."
        )

        def fake_compile(constraint, timeout_s):
            if constraint is None:
                return 0, "", {"flask": ("3.1.1",)}
            return 1, no_solution, {}

        monkeypatch.setattr("scripts.check_cve_blockers._compile_production", fake_compile)
        result = probe_production({"id": "X", "package": "flask", "fix_in": "99.0"})
        assert result.outcome == BLOCKED

    def test_baseline_compile_failure_is_unknown(self, monkeypatch) -> None:
        """If the UNCONSTRAINED compile fails, the probe is broken — that says nothing about
        the floor, and must not be reported as though it did."""

        def fake_compile(constraint, timeout_s):
            return 2, "error: network unreachable", {}

        monkeypatch.setattr("scripts.check_cve_blockers._compile_production", fake_compile)
        result = probe_production({"id": "X", "package": "flask", "fix_in": "3.1.3"})
        assert result.outcome == UNKNOWN
```

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run pytest src/tests/test_check_cve_blockers.py::TestScopeDispatch -q
```
Expected: `ImportError: cannot import name 'entry_scope'`.

- [ ] **Step 3: Implement**

```python
LOCK_SCOPE = "lock"
PRODUCTION_SCOPE = "production"
_SCOPES = frozenset({LOCK_SCOPE, PRODUCTION_SCOPE})


def entry_scope(entry: dict[str, str]) -> str:
    """Which resolution holds this entry's blocker. Defaults to the lock.

    Never a way to skip probing — it chooses the prober. An unknown value raises rather than
    defaulting, because probing the wrong resolution yields a confident wrong verdict.
    """
    scope = str(entry.get("scope", LOCK_SCOPE)).strip() or LOCK_SCOPE
    if scope not in _SCOPES:
        raise ProbeError(f"{entry.get('id')}: unknown scope {scope!r}; expected one of {sorted(_SCOPES)}")
    return scope


def probe_production(entry: dict[str, str], *, timeout_s: int = DEFAULT_TIMEOUT_S) -> Result:
    """Attempt the floor against the PRODUCTION resolution.

    Simpler than the lock probe: `uv pip compile -c` takes a constraints file, so nothing in the
    repo is mutated — no pyproject splicing, no finally-block restore, no residue check.

    Note this compiles FRESH, whereas the audit target in audit_resolutions.py fetches the
    DEPLOYED requirements.txt. That asymmetry is deliberate and must not be "fixed": the probe
    asks whether the NEXT deploy could take the fix; the audit asks what the CURRENT deploy is
    exposed to. They are different questions about different artifacts.
    """
    package = package_name(entry)
    floor = f"{package}>={entry['fix_in']}"
    with tempfile.TemporaryDirectory() as tmp:
        constraint = Path(tmp) / "c.txt"
        constraint.write_text(f"{floor}\n", encoding="utf-8")
        base_code, base_out, base = _compile_production(None, timeout_s)
        code, output, after = _compile_production(constraint, timeout_s)
    if base_code != 0:
        # The UNCONSTRAINED compile failing is a broken probe, not a blocked floor.
        return Result(str(entry["id"]), package, floor, UNKNOWN, f"baseline compile failed: {base_out[:200]}")
    changes, vanished = graph_changes(base, after, package) if code == 0 else ([], False)
    outcome, detail = classify(code, output, collateral=changes, target_vanished=vanished)
    return Result(str(entry["id"]), package, floor, outcome, detail, tuple(changes))
```

**Pass the real `returncode` and `output` to `classify`.** Hard-coding `0`/`""` would make BLOCKED
unreachable — an unsatisfiable production floor would come back UNKNOWN, indistinguishable from a
network failure, which is defect 2's confusion class reintroduced in the prober built to fix it.
`classify` already discriminates: it matches `_NO_SOLUTION = "No solution found when resolving"`
(`check_cve_blockers.py:122`) and treats any *other* non-zero exit as UNKNOWN.

Verified 2026-08-11 that `uv pip compile` emits exactly that string, so no new matching is needed:

```
$ printf 'flask>=99.0\n' > c.txt
$ uv pip compile pyproject.toml --extra taipy-app --python-version 3.10 \
      --python-platform linux -c c.txt
  × No solution found when resolving dependencies:
  ╰─▶ Because taipy-gui>=4.1.1 depends on flask>=3.1.0,<3.2 and flask>=99.0,
      we can conclude that taipy-gui>=4.1.1 cannot be used.
EXIT=1
```

`resolver_explanation()` captures that head unchanged, naming `taipy-gui` as the cap holder —
which is precisely what a `blocked_by` field is supposed to say.

Add:

```python
def _compile_production(
    constraint: Path | None, timeout_s: int
) -> tuple[int, str, dict[str, tuple[str, ...]]]:
    """Compile the production resolution. Returns (returncode, combined output, versions).

    The versions dict uses the same {name: (version, ...)} shape lock_versions produces, so
    graph_changes works against it unchanged. Tuple-valued because the LOCK can fork a package
    across conflicting extras; a compile never can, but keeping one shape keeps one comparator.
    """
    cmd = [
        "uv", "pip", "compile", "pyproject.toml", "--extra", "taipy-app",
        "--python-version", "3.10", "--python-platform", "linux",
    ]
    if constraint is not None:
        cmd += ["-c", str(constraint)]
    result = subprocess.run(  # noqa: S603 — fixed argv plus a generated temp path, no shell
        cmd, cwd=_REPO, capture_output=True, text=True, check=False, timeout=timeout_s
    )
    versions: dict[str, tuple[str, ...]] = {}
    for line in result.stdout.splitlines():
        match = _PIN_RE.match(line)
        if match:
            versions[normalize(match.group(1))] = (match.group(2),)
    return result.returncode, result.stdout + result.stderr, versions
```

with `_PIN_RE = re.compile(r"^([A-Za-z0-9._-]+)==([^\s;]+)")` as a module-level constant (project
convention: never compile a pattern inside a function body).

In `check_entry`, dispatch on `entry_scope(entry)` before the existing lock path.

- [ ] **Step 4: Run the tests**

```bash
uv run pytest src/tests/test_check_cve_blockers.py -q
```
Expected: PASS.

- [ ] **Step 5: Document `scope:` in `.pip-audit-ignores.yml`'s header**

Splice into the header comment (do NOT round-trip the YAML):

```yaml
#   scope: (optional, default `lock`) — which resolution holds this entry's blocker. `production`
#   probes `uv pip compile --extra taipy-app` instead of `uv lock`. It selects a prober; it never
#   exempts an entry from being probed.
```

- [ ] **Step 6: Full gate suite.**

---

# UNIT 3 — resolve five Dependabot PRs

## Task 7: Re-derive the open list

~14 `dependabot/*` branches exist and several are superseded. Do not trust a stale list.

- [ ] **Step 1:**

```bash
gh pr list --author "app/dependabot" --state open \
  --json number,title,headRefName -q '.[] | "#\(.number) \(.title)"'
```

Expected at time of writing — **five in scope**: `#512` dev-tooling group, `#511` hashicorp/aws,
`#510` databricks/databricks, `#505` openevolve, `#503` optuna-integration[mlflow].

**Explicitly excluded, do not resolve:**
- `#506` silly-kicks — **held** by operator decision; parallel work on that library continues.
- `#504` dbt-core 1.12.0 — **its own cycle.** It is not a version bump: it removes
  `dbt-semantic-interfaces` and adds `metricflow`, `sqlglot` and `dbt-core-experimental-parser`
  (a **pre-release alpha/beta**, seen as `2.0.0a5` on the branch and `2.0.0b1` in a fresh resolve)
  into a lock that ADR-046 feeds into the serverless environment. Its only real validation surface,
  `dbt-live-ci.yml`, is `schedule:` + `workflow_dispatch` with **no `pull_request` trigger**, so a
  parse regression lands green and breaks the next morning.

## Task 8: Resolve the two Terraform PRs

`#510` and `#511` touch the terraform lockfile only. They share nothing with `uv.lock` and are
verified by Terraform Plan.

- [ ] **Step 1:** For each of `#510`, `#511`: check out the branch, rebase on `main`.
- [ ] **Step 2:** `terraform -chdir=terraform/environments/dev init -backend=false && terraform -chdir=terraform/environments/dev validate`
- [ ] **Step 3:** Push; confirm the `plan` check passes on the PR.
- [ ] **Step 4:** Merge — **REQUIRES OPERATOR APPROVAL**, one at a time.

## Task 9: Resolve the three uv PRs

**Every Dependabot PR is incomplete as-opened**: under ADR-046 the terraform env pins mirror
`uv.lock`, and Dependabot does not run `sync_tf_env_pins.py`.

For each of `#512`, `#505`, `#503`, **one at a time**:

- [ ] **Step 1:** `gh pr checkout <N> && git rebase main`
- [ ] **Step 2:** `uv lock`
- [ ] **Step 3:** `uv run python scripts/sync_tf_env_pins.py && uv run python scripts/sync_tf_env_pins.py --check`
- [ ] **Step 4:** Run the full gate suite (Global Constraints).
- [ ] **Step 5:** Run the audit — this is what Unit 1 was for:

```bash
PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_resolutions.py
```
Expected: **all targets CLEAN**, including both Space targets — achievable because Task 5b deployed
the flask fix. If Task 5b has not completed, this criterion is unreachable and Unit 3 must wait.

A **FINDINGS** means the bump introduced an advisory; an **UNKNOWN** means the audit broke and must
be fixed before reading anything into it.

- [ ] **Step 6:** Push, confirm CI green, merge — **REQUIRES OPERATOR APPROVAL** per PR.

Do not batch. Prod deps are deliberately ungrouped in `.github/dependabot.yml` because batching
perturbs the conflict-fork resolution — a previous batch dragged env-pinned `mlflow-skinny` and even
downgraded dbt in the default fork.

---

## Self-Review

**1. Spec coverage.** §1.1 defect 1 (`--no-project`) → Task 1 Step 5. Defect 2 (exit-code mapping) →
Task 1. Defect 3 (dev group) → Task 2. Defect 4 (`--strict`) → Task 1 Step 5; the spec records it as
latent-by-construction, so no RED demonstration is required and none is claimed. §1.2/§1.3
(production surface unobserved) → Task 4. D1 → Task 1. D2 → Tasks 1–2. D3 (audit, do not converge) →
Task 4; convergence is absent from this plan by design. D4/D5 (flask fixable, carve-out) → Task 5.
D6/D9 (exclusions) → Task 7. D7 (terraform separate) → Task 8. D8 (lockstep) → Task 9. D10 (`scope:`
dispatch) → Task 6. §7 items are out of scope and appear in no task, correctly.

**2. Placeholder scan.** No TBDs. Every code step carries real code. Task 6 Step 3 describes
`_compile_production` in prose plus its exact signature and return shape rather than a full body —
the only such case, and deliberate: it is a mechanical transform of `lock_versions`, which the
implementer will have open.

**3. Type consistency.** `classify_audit -> AuditResult` (Task 1, rev 9) is consumed by `audit()` in
the same task and by Task 4's Space loop, which constructs one directly on its fetch-failure path.
`check_cve_blockers.py` gained its **own** `Outcome` enum in the same commit (rev 9a), keeping its
`BLOCKED` / `COLLATERAL` / `MOVED` / `UNKNOWN` names as module-level aliases. The two `Outcome`
types are deliberately **separate** — different member sets, different domains, and the two gates
fail independently — which a test asserts. Task 6's `entry_scope` / `probe_production` snippets
below therefore need no edit: the bare names they use now resolve to enum members, and
`classify` still returns `tuple[Outcome, str]`. `_export_cmd(extra) -> list[str]` (Task 2) is consumed only by
`export_resolution`, whose own signature is unchanged — B3 needs one flag, so `RESOLUTIONS` and
`label()` stay exactly as they are and no new type is introduced. `entry_scope -> str` and
`probe_production -> Result` (Task 6) reuse the existing `Result` dataclass and `classify` signature
verbatim; `_compile_production -> tuple[int, str, dict[str, tuple[str, ...]]]` matches what
`graph_changes` already consumes. `audit_resolution` changes return type in Task 1 and is not
re-changed later.

**4. Ordering.** Unit 1 → 2 → Task 5b → 3 is enforced by the Global Constraints and restated in
Task 9 Step 5, where the repaired audit is the acceptance check for each bump. Task 5b is the hinge:
it is Unit 2's definition of done and Unit 3's precondition.

**5. Open Decision C resolved as C2** — Task 5a adds the seam, Task 5b consumes it. The C1
detective assert is kept at Task 5b Step 4 as live confirmation. ADR-076 is written and ships with
Task 5a's commit.

**6. D5 is enforced exactly once**, at Task 5b Step 2, which is the only step in the cycle that
changes what the Space installs. Task 5 Step 1's diff tests the *constraint* at one moment and is
not a substitute.

**7. Local-verification blind spot.** Every local check in this plan runs against a `.venv` that
`uv run --no-project` silently layers onto (measured — see Task 4a). Task 4a Step 2's fresh-clone
check and the two CI dispatches (Task 3 Step 3, Task 4a Step 3) are the only verifications in the
plan capable of failing the way the runner fails. Do not substitute a local run for either.

---

## Revision history

**rev 9 (2026-08-11)** — during Task 1's implementation. **`audit()` discarded pip-audit's
stderr**, so the UNKNOWN this task exists to produce would have printed `pip-audit did not produce
a JSON report (exit 1)` and nothing else — telling the reader to fix the runner while withholding
the only evidence for doing so, in the very task whose purpose is that a failed run must be
legible. Rev 8's `tuple[str, str]` had no room to carry it. Replaced with a frozen `AuditResult`
(`outcome`, `detail`, `diagnostics`) — parallel to `check_cve_blockers.Result`, deliberately not
shared with it — plus `bound_diagnostics` (both ends kept, elision counted) and
`report_diagnostics` (printed on non-CLEAN only, since pip-audit writes progress to stderr).
`classify_audit` gains a `stderr` parameter used ONLY to populate the result;
`test_stderr_never_changes_the_verdict` is the executable form of that rule, because handing a
classifier the prose is how prose-matching starts. The three bare string constants became
`Outcome(str, Enum)` so a typo'd comparison is a type error. Also: rev 8's replacement summary
block silently dropped the pre-existing "add a `blocked_by` re-derived by EXECUTION, or take the
fix" guidance — restored, fired only when a FINDINGS is present. The same truncate-but-say-so rule
was applied to the FINDINGS package list (`and N more`). Verified by **execution**, not unit tests
alone: a forced unresolvable pin now names the package in the log. Task 4's Space-loop snippet and
Interfaces updated to the new shapes so an implementer does not unpack a tuple that no longer
exists. **The same enum treatment was applied to `check_cve_blockers.py` in the same commit** —
see below.

**rev 10 (2026-08-11)** — **every commit instruction removed from this document.** It carried
*"Commit — REQUIRES OPERATOR APPROVAL"* at the end of seven tasks plus two `git add` recipes. Those
were never operator-approved commits; they were a template habit, and they read as boundaries —
which is how Task 1 shipped as its own commit with Task 2 queued behind it, against the standing
rule that commits are minimal and per-unit. The operator's question was the right one: *why are
these in the plan at all, unless they are the exact commits I approved?* They were not. A task now
ends at its verification step, and the Global Constraints state that this document does not own
commit boundaries. The two surviving `REQUIRES OPERATOR APPROVAL` markers are on **merging
Dependabot PRs** (Tasks 8 and 9) — operator actions on pre-existing PRs, not boundaries this plan
invented.

**rev 9c (2026-08-11, Task 2's commit)** — the Global Constraints gate suite ran `pyright src/`,
which is **narrower than CI's** `pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py
scripts/sync_tf_env_pins.py` (`python-ci.yml:92`). A type error in the Taipy app or in the ADR-046
pin tooling was invisible to every local run this plan prescribes — the same shape as the defect
Unit 1 exists to fix: a check that looks like it covers something it does not. Corrected here and
in `CLAUDE.md`, whose Code Quality section had the same narrow command and listed only four of the
seven checks CI enforces (`lint-imports`, `bump_wheel --check` and `pip_audit_ignores --check` were
absent; verified present at `python-ci.yml:95`, `:117`, `:181`). Wider target executed before being
documented: 0 errors, 199 warnings.

**rev 9b (2026-08-11, same commit)** — Task 2's `TestDevGroupExclusion` specified a
`test_every_conflicting_extra_is_audited` that **`TestResolutionCoverage` already had**. The plan
had written the new class in isolation and never reconciled it against the module it appends to;
three review rounds examined that new material and caught real defects in it, but every one of
them judged the new code against itself, which structurally cannot surface a collision with
existing code. Rev 8's version was the better assertion — it compared declared extras against the
raw `RESOLUTIONS` (what `--extra` actually receives) rather than against `label()` (display-only,
and identity by coincidence, so a readability rename would fail a test over a change that broke
nothing) — so its comparison basis was merged into the existing test in Task 1's commit, together
with the existing failure message and a new non-empty assertion on the parsed extras, since a
regex that matches an empty block is how that test would quietly stop testing anything. Task 2 now
adds no coverage assertion. **Process rule for the rest of this plan: when a task appends tests to
an existing module, read that module first and state, per new test, whether it replaces, subsumes,
or sits beside one already there.**

**rev 9a (2026-08-11, same commit)** — operator decision: fold the sibling gate's conversion into
Task 1 rather than sequencing it as its own commit before Task 6. Task 6 is the only task in the
cycle that opens `check_cve_blockers.py`, so there was no existing commit that *touched* it — but
Task 1 is the commit that *owns the decision*, and stating one rule once, applied to both gates in
one diff, beats the same rationale appearing twice five commits apart. It also means Task 6 writes
its new `probe_production` comparisons against the enum from birth instead of adding three more
unprotected ones. `BLOCKED` / `COLLATERAL` / `MOVED` / `UNKNOWN` became
`check_cve_blockers.Outcome`, with module-level aliases retained so the ~20 existing reference
sites do not churn in a commit whose point is that behaviour does not change; `Result.outcome` and
`classify`'s return type are now `Outcome`. The weekly log's `f"{result.outcome:10s}"` column is
pinned byte-identical by `test_outcome_log_column_is_unchanged`, because a bare `Enum` renders
`Outcome.MOVED` and a `str` mixin without the `__str__` override disagrees between `str()` and an
f-string on 3.10 — both silent regressions in the only output a human reads. The two gates keep
**separate** `Outcome` types (`test_the_two_gates_keep_separate_outcome_types`): same idiom,
different member sets, and they fail independently. `classify()` deliberately still returns
`tuple[Outcome, str]` rather than a result object — the outcome *type* is the safety-relevant
part, and widening further has no failure mode behind it. Task 6's snippets keep using the bare
`BLOCKED` / `UNKNOWN` names, which now resolve to enum members.

**rev 8 (2026-08-11)** — the D5 manifest was **executed** under Git Bash on Windows across six
scenarios (quoting held; the defects were logical). Two serious: the carve-out keyed on package
**name**, so `taipy-rest 4.1.1 -> 1.0.0` passed and was issued a digest — now keyed on the exact
**transition**; and an empty manifest was a pass, so a stale artifact identical to production
sailed through green and the sha256 seam would have faithfully certified the unfixed bytes —
the manifest now asserts `flask==3.1.3` is actually present, which is the "stale checkout" failure
Task 5b Step 1 names and the gate could not previously catch. Also: a missing requirements file
gave a raw `FileNotFoundError` traceback and now explains itself; the em-dash in the failure
message violated the ASCII-only rule for `-c` payloads and mangled on the default console;
`PYTHONIOENCODING=utf-8` exported; removed a trailing instruction to add imports the script already
had.

**rev 7 (2026-08-11)** — after partial review of Task 5a and ADR-076. **The `--expect-sha256` guard
was vacuous**: Step 3 derived the digest with `sha256sum` from the same file `_prepare_requirements`
hashes, in the mode that never rewrites it, so it asserted `sha256(file) == sha256(file)` and could
not fail. The manifest now emits `VALIDATED_SHA256=` and Step 3 consumes that, binding the
expectation to the validation rather than to the artifact; ADR-076's first Positive bullet was
claiming the undelivered property and now states the condition. **`deploy production` without
`--no-compile` is refused**, following `_require_force_for_production` (`:299-303`) — the guarantee
is structural on production, opt-in on staging where the artifact is produced. `_build_parser()`
extracted so flags are assertable without deploying. `relative_to(repo_root)` wrapped against the
tests' own monkeypatch. ADR-076's Notes described the **grep** test the plan had already rejected;
now describes the AST one, with a do-not-simplify warning. **ADR-076 alternative B rejected for the
wrong reason**: the Dependabot-spam premise expired at #450 when the `pip` ecosystem was removed —
committing the file today produces zero PRs — so B is now rejected on drift and two-pin-sets
grounds, with the expiry recorded; `.gitignore`'s comment gets the same correction.

**rev 6 (2026-08-11)** — self-audit of rev 5's new Task 5a, before review. Three defects, all mine:
`test_compile_is_called_from_exactly_one_place` counted a source string and would have failed on
*three* counts (the current file already has 3 occurrences, and `_prepare_requirements`' own
docstring adds a fourth) — **the identical mistake rev 4 fixed in `test_the_flag_is_unconditional`**,
made again one task later; now parsed with `ast`. `main()` is `def main() -> int` reading `sys.argv`,
so the CLI test would have raised `TypeError` not `SystemExit`; Step 4 now adds `argv` matching
`audit_resolutions.main`. And `_compile_requirements` rebuilt the requirements path independently of
the new `_requirements_path()` — a second construction site for one value, inside the commit whose
ADR cites ADR-075 against exactly that.

**rev 5 (2026-08-11)** — operator put **C2 in scope**. New **Task 5a** adds a `--no-compile` /
`--expect-sha256` seam to `manage_space.py`, routing both former `_compile_requirements()` call
sites through a single `_prepare_requirements()`, so one validated pin set ships to both Spaces
(`--no-compile` without a hash is rejected by the parser — fail-closed). The deploy task became
**Task 5b** and now passes the dry-run's digest to both deploys; its staging≡production assert is
retained as live confirmation. A source-level test pins `_compile_requirements()` to exactly one
call site, so the two-site defect cannot return. **ADR-076** written (`Proposed`); Task 5a Step 6
flips it to `Accepted` and commits it with the code.

**rev 4 (2026-08-11)** — after partial review of rev 3, scoped to what rev 2/3 introduced.
`test_the_flag_is_unconditional` **raised `IndexError`** — its source-scraping split truncated at the
docstring's own mention of `--no-default-groups`; replaced with an exhaustive-input loop plus a
signature assertion, no text scraping. Task 5b Step 2 was a three-package grep and is now D5's full
change manifest with a fail-on-backwards-move exit. Task 4a Step 2's GREEN command was unpinned and
installed 1.27.0 while the workflow ships 1.6.0 — now uses the shipped pin, with the rationale in a
YAML comment. B3's covering set corrected 218 → **216** (markers evaluated for linux; the raw-export
proxy inflated it in the direction that favours the conclusion) and the four dev-side counts
re-derived; conclusion unchanged, 0 uncovered. The reviewer's causal claim about `pywin32`/`waitress`
was corrected: they reach python-ci only via the analytics-family extras and appear in none of this
job's four resolutions, so B3 does not create that gap. New **Open Decision C** — staging is not a
real gate, because every deploy re-resolves.

**rev 3 (2026-08-11)** — Open Decisions A and B resolved by the operator. **A1**: the production
deploy is in scope, added as Task 5b between Units 2 and 3; Task 9 Step 5's pass criterion is now
reachable. **B3**: no dev target — measurement showed all four forks' dev-side packages sit inside
what `python-ci.yml` audits on every PR (0 uncovered), so Task 2 shrank from a new `Target` type to
one unconditional `--no-default-groups` flag, and `RESOLUTIONS`/`label()` stay untouched.

**rev 2 (2026-08-11)** — after external review. Five defects confirmed by execution and repaired:
the workflow lacked `huggingface_hub` and no local check could reveal it (Task 4a, new);
`probe_production` hard-coded `classify(0, "")` and so could never return BLOCKED (Task 6);
`resolve_hf_token()` returns `""` not `None`, reaching `hf_hub_download` as the documented
empty-Bearer footgun (Task 4); the dev-target package count was 237 and is 182 (Task 2); `--only`
lost its unknown-name rejection (Task 4). Minor: dropped an unused `_PASSING_AUDIT`, added the
`non-zero exit + parseable + no vulns → UNKNOWN` branch, de-raced the `gh run list` poll, recorded
that `uv lock` is a no-op and why. Two findings were NOT resolved in-plan because they change
scope — they are Open Decisions A and B above.
