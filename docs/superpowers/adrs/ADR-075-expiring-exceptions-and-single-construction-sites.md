# ADR-075: Exceptions carry an expiry, and a cross-cutting concern gets one construction site

**Status:** Accepted
**Date:** 2026-08-10 (amended 2026-08-11 — a revisit condition that only a human can evaluate is still unobserved)
**Deciders:** Karsten Nielsen
**Generalises:** the `review_trigger` field introduced for `.pip-audit-ignores.yml` in [ADR-074](ADR-074-hf-sync-process-isolation-and-memory-observability.md)
**Related:** [ADR-046](ADR-046-serverless-env-exact-pins.md) (pure-core fixer≡checker), [ADR-071](ADR-071-ci-databricks-oidc-auth.md) (CI Databricks auth)

## Context

Two problems surfaced in the same cycle, and they turned out to be one problem.

**1. A justified exception outlives its justification.**

`scripts/_bronze_table_inventory.py` began as `table -> reason`: four `*_pre_*_backup` tables
excluded from the bronze contract because they are LL2 Path B migration backups with no model,
no reader and no `sources.yml` entry. Every entry had a *why* and no *until when*.

The evidence that this fails was already on disk. Those backups were justified on **2026-04-29**
and were still present on **2026-08-10** — 103 days — because nothing was watching. The phrase
"classifying a table is not the same as endorsing it" is false without a deadline: **without an
expiry, classification IS endorsement.**

`.pip-audit-ignores.yml` had already solved this with `review_trigger`, and this repo had spent
three review rounds establishing that the field is load-bearing rather than decorative. The
defect was reproduced two files away from its own fix.

**2. A cross-cutting concern with 26 construction sites fails 26 different ways.**

`~/.databrickscfg` holds both a `DEFAULT` and an `OAUTH` profile matching the workspace host, so
a bare `WorkspaceClient()` raises at construction with `... Use --profile to specify which
profile to use` — advice naming a flag most scripts do not have. Applying the PR-2a bronze
migration hit this: it fired *after* the operator had substituted the migration's cutoff and
believed the apply was under way, so a pure auth failure read as a partial write.

PR-2a fixed the one runner and deferred the rest. Enumerating "the rest" was wrong three times —
22 (a `tail`-truncated listing), then 21 (a regex), then **26** (AST). The regex produced nine
false positives: a *parameter* or *local* named `workspace_client`, a `--profile` that was a
subprocess argument to the `databricks` CLI, and private `_workspace_client()` wrappers around
bare clients.

## Decision

**Every deliberate exception records the condition or date under which it stops being one, and
something automated notices when that arrives.**

Exception registries carry, per entry: a **reason**, a **`review_trigger`** (the condition that
would end it), and — where the exception is temporally scoped by its own nature — a
**`classification`** (`TEMPORARY` / `PERMANENT`), a **`recorded`** date, and a **deadline** past
which a test fails. `NON_CONTRACT_TABLES` uses 180 days.

The registry is a **partition, not a filter**: every live item is classified into exactly one
bucket, and anything in neither fails a completeness assertion. A name heuristic (`_backup`,
`_pre_*`) is not acceptable — it silently misses the next one named differently.

**A cross-cutting concern gets one construction site**, and exceptions to it are an explicit,
reasoned set rather than an absence. `ingestion.databricks_auth.workspace_client()` is the sole
constructor for every `scripts/` Databricks client; it installs `CachedGitHubOidcStrategy` under
GitHub OIDC, returns a stock client otherwise, and converts the ambiguous-profile `ValueError`
into a message naming `DATABRICKS_CONFIG_PROFILE` (which the SDK reads natively —
`Config.profile` is `ConfigAttribute(env=...)`) and stating plainly that **nothing ran**.

**Membership in such a set is decided by AST, never by regex over file text.**

## Alternatives considered

**Name-heuristic filtering for non-contract tables** (`_backup`, `_pre_*`). Rejected: it is the
same class of classifier that produced nine false positives in the auth enumeration, and it
fails silently on the next table named differently — the exact failure mode the registry exists
to prevent.

**Documenting the backups instead of excluding them.** Rejected: ~988 columns of `sources.yml`
for four tables nothing reads.

**Deriving the contract set from a hand-maintained list.** Rejected: a second source of truth
beside `sources.yml`, free to drift. The contract is *derived*; only the exceptions are declared.

**Per-script `--profile` flags** for the operator scripts (the original plan). Rejected once the
SDK was verified to read `DATABRICKS_CONFIG_PROFILE` natively: sixteen argument-parser surgeries
would have bought a better error message and nothing else.

**A `scripts/`-only auth wrapper**, to avoid a wheel bump. Rejected — the no-bump premise was an
inference nobody had asked for, and preserving it would have created a second helper beside the
existing one.

**Making `blocked_by` machine-checkable from `uv.lock` or `importlib.metadata`.** Both rejected
by measurement: the lock records dependency *names* without specifiers, and
`importlib.metadata` reads *installed* distributions — four of five cap-holders are not
installed. Recorded here because both looked correct until run.

## Consequences

### Positive

- A forgotten exception now fails a test instead of aging silently. The four LL2 backups have a
  real deadline: **2026-10-26**.
- A new bronze table is in neither bucket and fails the completeness assertion, forcing a
  conscious contract/non-contract decision.
- One auth error path, one message, one place to improve it. CI-reachable scripts additionally
  gained the OIDC caching they should have had.
- Extending the partition to all six providers immediately found **metrica carries the identical
  backup pair** — the instance-vs-class payoff, in the same commit.

### Negative

- `test_no_temporary_exclusion_is_overdue` is **time-dependent** — the only such test in the
  suite. It will fail on a date rather than on a code change. That is the intended mechanism,
  but it means CI can go red without anyone touching the repo.
- The auth consolidation was a 26-file mechanical edit, the riskiest change in its commit. It
  broke one file (import inserted inside a multi-line `from … import (`) and introduced an
  `F823` shadowing bug; both were caught by ruff, but a similar sweep should expect the same.
- `workspace_client()` now sits on the import path of many operator scripts, so `scripts/` →
  `ingestion` coupling is wider. Legal (`.importlinter` covers only the four wheel packages) and
  already precedented, but it means those scripts need the wheel installed.

  **This consequence was written down and then not acted on, and it broke `main` on
  2026-08-11.** `terraform-apply.yml` runs `scripts/patch_job_retries.py` with a bare `python`
  and `pip install databricks-sdk requests` — no wheel — so the rewritten import raised
  `ModuleNotFoundError: No module named 'ingestion'`. Terraform Apply itself succeeded; the
  ADR-025 post-apply retry patch did not run. Worse, that script's docstring **already carried
  the reason** ("terraform-apply.yml runs this with a bare `python` … without the project wheel
  on sys.path") and the sweep edited the code three lines below it while leaving the paragraph
  standing. A fence with its sign still attached.

  Two corrections follow. **(1) A shared helper must live where every consumer can reach it, or
  the consumers that cannot must be an explicit, reasoned set.** `scripts/` holds two classes:
  application/ops scripts that legitimately carry the project environment, and CI plumbing that
  runs in a deliberately minimal one. The second class stays on a bare client — handing it the
  whole project graph (uv setup + dbt-packages materialisation + `uv sync`, ~4 steps on every
  Terraform Apply) to obtain one constructor inverts the cost. **(2) The gate that pushed the
  bad edit was half-observed**: `test_workspace_client_construction.py` asserted CI scripts
  *import* the helper and never that their runtime could *resolve* it. `test_ci_script_environment.py`
  closes the other half, structurally and with no allowlist — a script that does not import a
  wheel package is simply not subject to it.

  **Amended 2026-08-12 — that gate did NOT close the half it claimed, and the same sweep had
  already broken two more scripts.** `_provides_project` treated `--no-project` as the only form
  that withholds the environment, and passed `dbt-live-ci.yml` by *three* independent routes:
  `uv run --no-sync` (declines to sync), `uv sync --no-install-project` counted as an install
  (it is the flag that means the opposite), and a fallback matching the workflow's RAW TEXT —
  so the line-36 comment *"the extra 10 minutes covers checkout, uv sync, tarball"* alone marked
  the workflow as provisioned. Prose could silence the gate. The nightly died on
  `upload_ci_shim.py` the first time it ran after #518, and the repaired guard immediately
  surfaced `trigger_dbt_job.py` as a second offender that had never been reached only because
  the shim step failed first. **`trigger_dbt_job.py`'s own `_build_auth_header` docstring said
  "deliberately inlined rather than importing `ingestion.databricks_auth`" — the third fence
  with its sign still attached that this sweep edited past.** The lesson generalises past this
  gate: a predicate must enumerate the conditions that WITHHOLD a capability, not just the one
  that named itself after doing so.

### Neutral

- **Five** documented exemptions are pinned as a set with reasons:
  `scripts/ci/run_dbt_in_databricks.py` (ambient Databricks runtime auth, where no profile is
  resolved), `scripts/migrations/_runner.py` (its own `--profile` and try/except), — added
  2026-08-11 — `scripts/patch_job_retries.py` (runs in a wheel-free CI step under
  `DATABRICKS_AUTH_TYPE=github-oidc`, where no `~/.databrickscfg` exists and the profile
  ambiguity cannot arise), and — added 2026-08-12 — `scripts/upload_ci_shim.py` and
  `scripts/trigger_dbt_job.py`, both for the same reason in `dbt-live-ci.yml`
  (`uv sync --no-install-project` then `uv run --no-sync`, also under `github-oidc`).
  The count went 3 → 5 in a single day, which is itself the signal: **the wheel-free CI class is
  larger than the sweep assumed**, and each member was discovered by breaking rather than by
  enumeration.
- The 180-day window is a judgement, not a derived value. Chosen so the change ships green
  rather than landing the suite red on a decision only an operator can take.

## Amendment (2026-08-11) — a revisit condition that only a human can evaluate is still unobserved

The decision above requires every exception to record the condition under which it stops being
one. Applying it to `.pip-audit-ignores.yml` the next day showed that the requirement is not
sufficient: **all six entries already had a `review_trigger`, and three of them named the wrong
upstream.**

- `python-dotenv` blamed `taipy`, which does not declare python-dotenv at all — the cap is
  taipy-gui's.
- `deepdiff` recorded `<8`; the measured cap is `<=7.0.1`, so 7.0.2 was blocked too.
- `pyarrow` blamed a *"databricks-connect/pyspark compatibility pin"*. **Neither package is in
  `uv.lock`.** Its `review_trigger` — "databricks-connect supporting pyarrow 23+" — watched an
  upstream that could never have unblocked it. The real holder is taipy-core (`pyarrow<19.0`).

Each of those was true, or plausible, when written. Prose does not stay true, and a
`review_trigger` phrased against the wrong package is *worse* than none: it looks discharged.

**So: where a revisit condition can be evaluated by execution, it must be.**
`scripts/check_cve_blockers.py` attempts each entry's floor for real and
`.github/workflows/cve-blocker-review.yml` re-runs it weekly. Three further rules, each of which
was a bug first:

1. **Assert the claim, not the mechanism.** The gate was designed to assert *"flooring D is
   unsatisfiable"* — and its first run disproved that on 2 of 6 entries. `setuptools>=83` and
   `cryptography>=50` both *resolve*, buying it with collateral downgrades their own
   justifications had described in prose. The uniform, decision-relevant claim is **"taking this
   fix is not free"**: outcomes are `BLOCKED` / `COLLATERAL` / `MOVED` / `UNKNOWN`, and the first
   two pass. Shipped as designed, the gate would have filed two false alarms on day one.
2. **Unverifiable is not verified-good.** A resolve can fail because the runner was offline. Only
   uv's own "no solution" wording counts as `BLOCKED`; every other failure is `UNKNOWN` and fails
   the job exactly as loudly as `MOVED`. Folding the two together would let an offline runner
   certify every blocker as intact — the inverse of ADR-068's fail-open rule, in the direction
   where fail-open is wrong.
3. **A constraint can be satisfied by the target leaving the graph.** `setuptools>=83` resolves
   because torch backtracks to 2.10.0, which does not depend on setuptools at all. A pre-flight
   presence check cannot see this: the package is there when the probe starts and gone when it
   ends. Verify post-resolve.

**Audit RESOLUTIONS, not the environment that happens to be installed.** The scope of
`.pip-audit-ignores.yml` was originally "what pip-audit reports in the CI-synced environment",
and that boundary turned out to be a bug rather than a definition. `uv run pip-audit` audits the
base resolution plus the dev group. **Production is not that environment**: the Taipy Space
deploys with the `taipy-app` extra, which measured 2026-08-11 carries **17 findings across 11
packages** against base's 7 across 5 — eight advisories CI had never once looked at, matching
one-for-one the eight Dependabot alerts that had no in-repo home.

An installed-environment audit *cannot* cover this project, structurally: `taipy-app` / `dbt` /
`sdk` are declared **conflicting** extras, so no single environment can hold them and for three
of the four resolutions there is nothing to install. `scripts/audit_resolutions.py` exports each
fork with `uv export --extra …` and audits it, driven weekly beside the blocker probe.

Consequences worth stating rather than burying:

- **The proxy is visible.** A PEP 440 local version exists on no index, so PyPI cannot audit it
  (`torch==2.11.0+cu128`). `--extra-index-url` was measured and does **not** help — pip then
  resolves it but pip-audit still reports *"Dependency not found on PyPI and could not be
  audited"*, leaving torch unaudited exactly as before. The local segment is therefore stripped
  and `torch==2.11.0` audited as a proxy; the substitution is logged on **every** run, because a
  security gate that silently substitutes its input is the failure mode this repo keeps paying
  for. Without it torch is skipped entirely, which is what CI did before and is strictly worse.

  **Amended 2026-08-12 (`--disable-pip`).** This bullet originally read *"`pip-audit -r`
  dry-run-installs the file, so a … local version … aborts the whole audit"*. That install was
  never wanted — `audit()` passed `--no-deps` but not `--disable-pip`, and pip-audit 2.10.1 gates
  its venv-free path on the latter alone (`_dependency_source/requirement.py:161`), so it built a
  throwaway venv and installed each locked resolution to rediscover versions the file already
  pinned. On the runner that venv's `ensurepip` exits 1, which is why every target of
  `cve-blocker-review.yml` classified UNKNOWN from the day the workflow was created. With the flag
  the install is gone; a local version now costs **the one package** (`--strict` still fails the
  run) instead of the whole audit. The strip stays — the blast radius shrank, the need did not.
- **Weekly, not per-PR.** Four resolutions cost ~15–20 min against a Python CI job that finishes
  in ~8–10. Dependabot already reads `uv.lock` and alerts the moment a vulnerable pin merges —
  it is how all eight were found. This job is the *enforcement* half (an alert must become a
  justified entry or a fix), which is a weekly question.

  **Amended 2026-08-12: the COST half of this rationale no longer holds.** The ~15–20 min was
  almost entirely the dry-run install corrected in the bullet above; with `--disable-pip` all four
  resolutions complete in **10 s** locally on a warm uv cache and **8.1 s on the CI runner**
  (run `31592589093`, `11:37:04.296` → `11:37:12.407`, cold runner with linux markers — measured,
  not projected. The first draft of this bullet guessed CI would be *slower* than local because it
  pays a pip-audit download and PyPI queries; it is marginally faster, so the guess is recorded as
  wrong rather than quietly dropped). The *second* half stands unchanged and was always the
  stronger argument:
  "has upstream moved?" is a question about events outside this repo, so its answer cannot change
  between two commits of ours. **This is recorded, not re-decided** — moving the job per-PR is a
  scope decision for the operator, and nothing here should be read as having taken it.
- **One blocker explains nearly all of it.** Seven of the eight trace to Taipy 4.1.1 pinning its
  own tree (taipy-gui caps flask-cors, markdown and twisted; taipy-rest caps marshmallow); the
  eighth is our own cu128 index capping torch.

**A "no fix exists" entry still needs its exploitability investigated, not assumed.** Taipy's own
`PYSEC-2026-3081` is an unauthenticated path traversal in `ElementLibrary.get_resource()` with no
released fix — taipy-gui 4.1.2, one version above our pin, was read directly and still carries
the flawed `str(file).startswith(str(base))`. Reachability was then established rather than
guessed, in both directions: the app *does* register a custom `ElementLibrary` that does not
override the method, but the documented escape needs a sibling directory whose name **extends** a
library's module directory, and neither the only built-in subclass (`_GuiCore` at
`taipy/gui_core`) nor ours (`.../extensions/ll_ext`) has one. The vulnerable code runs; the escape
has no target. That conclusion is **layout-dependent**, so the entry's `review_trigger` names the
sibling-directory condition alongside the upstream release — one new directory would silently
make it exploitable.

**Also learned, and generalisable:** `uv.lock` pins 9 of 298 packages **twice**, because the
declared extra `conflicts` fork the resolution. Two of them are entries in this very file. Any
tool reading versions out of `uv.lock` with a `{name: version}` map watches one fork blind.

## Amendment (2026-08-11, second) — a rule stated for one gate is not a rule the codebase has

Amendment 1's rule 2, *"unverifiable is not verified-good"*, was written about
`check_cve_blockers.py`. Its sibling `scripts/audit_resolutions.py` shipped in the **same cycle**
without it: `main()` mapped any non-zero exit to "unignored findings". The first execution on a
GitHub runner therefore printed

```
FileNotFoundError: Forced include not found: .../dbt_project/dbt_packages
FAIL: unignored findings in 4 resolution(s): base, taipy-app, dbt, sdk
```

— **four fabricated CVE regressions from one missing directory.** The ADR had the rule; one of the
two implementations did not. Writing a principle down next to the gate that motivated it does not
propagate it to the gate written beside it.

Five rules now bind every gate in this family, and each is enforced by a test rather than by this
document:

1. **The verdict comes from the tool's structured report** — never from the exit code alone, and
   never from matching prose. An upstream tool's wording is not an API. Where a signal genuinely
   must count, route it structurally: `--strict` turns "this dependency could not be collected"
   into a non-zero exit, so the gate never has to read English to find it.
2. **UNKNOWN is a distinct, loud outcome.** It fails the job, and it is reported as an
   infrastructure failure rather than as findings. Collapsing it into either neighbour is a lie in
   one direction or the other.
3. **The evidence travels with the verdict.** `audit_resolutions` discarded pip-audit's stderr at
   the subprocess boundary, so an UNKNOWN read *"pip-audit did not produce a JSON report (exit 1)"*
   and nothing else — telling the reader to fix the runner while withholding the only evidence for
   doing so. Dropping output at the boundary is irreversible; declining to print it is a policy the
   caller can revisit. Carry it, print it on any non-clean verdict.
4. **Outcomes are enums, not bare strings.** The failure directions are asymmetric: in
   `check_cve_blockers`, a silently-False `Result.ok` reports a justified ignore as failing (noisy,
   safe), while a silently-False `== MOVED` stops the gate reporting that a blocker has moved —
   quiet, and the entire purpose of the tool. The two gates keep **separate** `Outcome` types: same
   idiom, different member sets, and they fail independently.
5. **Where output is elided, say how much.** Both the diagnostic bound and the named-package list
   state what they dropped. A silent truncation is a quieter version of rule 3's bug.

This is the same shape as the finding that produced this ADR in the first place — a claim that was
true where it was written and untrue one file over — applied to the ADR's own rules.

## Related

- **Specs:** `docs/superpowers/specs/2026-08-10-bronze-coverage-completion-and-cve-floors-design.md`,
  `docs/superpowers/specs/2026-08-11-audit-convergence-and-dependabot-design.md` (amendment 2)
- **Plans:** `docs/superpowers/plans/2026-08-10-bronze-coverage-completion-and-cve-floors.md`,
  `docs/superpowers/plans/2026-08-11-audit-production-surface-and-dependabot.md` (amendment 2)
- **Enforced by:** `src/tests/test_bronze_table_inventory.py`,
  `src/tests/test_workspace_client_construction.py`, `src/tests/test_pip_audit_ignores.py`,
  `src/tests/test_check_cve_blockers.py`, and — weekly, outside the test suite —
  `.github/workflows/cve-blocker-review.yml` running `scripts/check_cve_blockers.py`
- **ADRs:** generalises ADR-074's `review_trigger`; shares the pure-core fixer≡checker shape with
  ADR-046; complements ADR-071 on the CI auth path.

## Notes

The two halves were written as one decision because they failed the same way. An exception
without a revisit condition and a cross-cutting concern without a single construction site are
both **states that nothing observes** — and in both cases the repo already contained the fix
(`review_trigger`; `workspace_client()`), applied to one instance and not generalised.

The recurring lesson, recorded because it cost four separate corrections in this cycle: an
enumeration asserted from a truncated read or a text heuristic is worse than an admitted
unknown, because it looks derived. Count with the AST, and re-run the command at implementation
time rather than trusting a number written days earlier.
