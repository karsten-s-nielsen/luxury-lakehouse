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

### Neutral

- Two documented exemptions remain and are pinned as a set with reasons:
  `scripts/ci/run_dbt_in_databricks.py` (ambient Databricks runtime auth, where no profile is
  resolved) and `scripts/migrations/_runner.py` (its own `--profile` and try/except).
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

**Scope boundary (a convention with downstream consumers).** `.pip-audit-ignores.yml` suppresses
what **pip-audit reports in the CI-synced environment** — not what Dependabot reports from
`uv.lock`. On 2026-08-11 those sets were 7 findings vs 14 alerts, because `uv run pip-audit`
audits an environment without the `taipy-app` extra and skips torch's `+cu128` build entirely.
An entry for an advisory pip-audit does not report is an inert flag whose "still suppresses a
live finding" property is false the day it is written. Do not add one.

That boundary has a consequence worth stating plainly rather than burying: **the CI audit does
not cover what production runs.** The Taipy Space deploys *with* the `taipy-app` extra, so eight
open advisories — including one high-severity issue in Taipy itself with no patched release —
reach production unaudited. Recorded here as a known gap, not fixed by this ADR.

**Also learned, and generalisable:** `uv.lock` pins 9 of 298 packages **twice**, because the
declared extra `conflicts` fork the resolution. Two of them are entries in this very file. Any
tool reading versions out of `uv.lock` with a `{name: version}` map watches one fork blind.

## Related

- **Specs:** `docs/superpowers/specs/2026-08-10-bronze-coverage-completion-and-cve-floors-design.md`
- **Plans:** `docs/superpowers/plans/2026-08-10-bronze-coverage-completion-and-cve-floors.md`
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
